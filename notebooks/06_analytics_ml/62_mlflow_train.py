# Databricks notebook source
# MAGIC %md
# MAGIC # Part 6.2 — MLflow: эксперимент, модель, реестр в Unity Catalog
# MAGIC
# MAGIC Для Data Engineer здесь важнее не сама модель, а **контур**:
# MAGIC
# MAGIC * эксперимент и run'ы — воспроизводимость (что за данные, какие параметры);
# MAGIC * сигнатура модели — контракт входа/выхода, без неё инференс ломается молча;
# MAGIC * реестр в UC — модель как объект каталога, с правами и версиями;
# MAGIC * алиас `@champion` — стабильная ссылка, по которой скорит батч-джоба.
# MAGIC   Переключение чемпиона не требует правки кода инференса.

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821
dbutils.widgets.dropdown("register_model", "true", ["true", "false"])  # noqa: F821

# COMMAND ----------

import mlflow
import mlflow.sklearn
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

from ecom.config import Config
from ecom.utils.logging import get_logger

log = get_logger("train")
cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
register = dbutils.widgets.get("register_model") == "true"  # noqa: F821

# Реестр моделей — в Unity Catalog, а не в устаревшем workspace-реестре
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Shared/ecom_churn_{cfg.catalog}")

MODEL_NAME = cfg.ml_churn_model
LABELS = cfg.table("ml", "customer_labels")

# COMMAND ----------

# MAGIC %md ## Данные
# MAGIC Датасет маленький (тысячи строк), поэтому обучаем на pandas.
# MAGIC На больших объёмах здесь был бы Spark ML или обучение на выборке.

# COMMAND ----------

df = (
    spark.table(cfg.ml_customer_features)  # noqa: F821
    .join(spark.table(LABELS), on=["customer_id", "as_of_date"])  # noqa: F821
    .toPandas()
)
print(f"строк: {len(df)}, доля оттока: {df['churned'].mean():.1%}")

CATEGORICAL = ["country", "segment"]
NUMERIC = [
    "tenure_days",
    "orders_cnt",
    "items_cnt",
    "total_spent",
    "avg_item_amount",
    "categories_cnt",
    "channels_cnt",
    "avg_quantity",
    "cancelled_items",
    "recency_days",
    "active_span_days",
    "orders_last_30d",
    "spent_last_30d",
    "sessions_cnt",
    "events_cnt",
    "add_to_cart_cnt",
    "remove_from_cart_cnt",
    "checkout_cnt",
    "cart_conversion_rate",
]
FEATURES = CATEGORICAL + NUMERIC

X = df[FEATURES].copy()
for col in NUMERIC:
    X[col] = pd.to_numeric(X[col], errors="coerce").fillna(0.0).astype("float64")
for col in CATEGORICAL:
    X[col] = X[col].fillna("unknown").astype(str)
y = df["churned"].astype(int)

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.25, stratify=y, random_state=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Обучение с автологированием
# MAGIC `mlflow.sklearn.autolog()` сам пишет параметры, метрики и артефакты.
# MAGIC Явно добавляем только то, чего он знать не может: датасет и дату отсечки.

# COMMAND ----------

mlflow.sklearn.autolog(log_models=False, silent=True)

preprocessor = ColumnTransformer(
    transformers=[("cat", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL)],
    remainder="passthrough",
)

with mlflow.start_run(run_name="hgb-churn") as run:
    model = Pipeline(
        steps=[
            ("prep", preprocessor),
            (
                "clf",
                HistGradientBoostingClassifier(
                    max_iter=200, learning_rate=0.08, max_depth=6, random_state=42
                ),
            ),
        ]
    )
    model.fit(X_train, y_train)

    proba = model.predict_proba(X_test)[:, 1]
    metrics = {
        "test_roc_auc": roc_auc_score(y_test, proba),
        "test_pr_auc": average_precision_score(y_test, proba),
        "churn_base_rate": float(y.mean()),
    }
    mlflow.log_metrics(metrics)
    mlflow.log_params(
        {
            "features_table": cfg.ml_customer_features,
            "labels_table": LABELS,
            "as_of_date": str(df["as_of_date"].max()),
            "n_rows": len(df),
        }
    )

    # Сигнатура — контракт модели. Без неё батч-инференс упадёт на первом же
    # несовпадении типов, причём уже в проде.
    signature = mlflow.models.infer_signature(X_train, model.predict_proba(X_train)[:, 1])
    mlflow.sklearn.log_model(
        model,
        artifact_path="model",
        signature=signature,
        input_example=X_train.head(3),
        registered_model_name=MODEL_NAME if register else None,
    )

    run_id = run.info.run_id

log.info("run=%s metrics=%s", run_id, metrics)
metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Промоушен чемпиона
# MAGIC Новая версия становится `@champion` только если она лучше текущей.
# MAGIC Это тот самый gate, которого обычно не хватает в учебных проектах.

# COMMAND ----------

if register:
    from mlflow.tracking import MlflowClient

    client = MlflowClient()
    versions = client.search_model_versions(f"name = '{MODEL_NAME}'")
    latest = max(versions, key=lambda v: int(v.version))

    try:
        champion = client.get_model_version_by_alias(MODEL_NAME, "champion")
        champion_auc = client.get_run(champion.run_id).data.metrics.get("test_roc_auc", 0.0)
    except Exception:
        champion, champion_auc = None, -1.0

    new_auc = metrics["test_roc_auc"]
    log.info("новая версия %s AUC=%.4f, чемпион AUC=%.4f", latest.version, new_auc, champion_auc)

    if new_auc > champion_auc:
        client.set_registered_model_alias(MODEL_NAME, "champion", latest.version)
        log.info("версия %s назначена чемпионом", latest.version)
    else:
        client.set_registered_model_alias(MODEL_NAME, "challenger", latest.version)
        log.info("версия %s осталась челленджером", latest.version)

    client.update_model_version(
        name=MODEL_NAME,
        version=latest.version,
        description=(
            f"HistGradientBoosting, ROC-AUC={new_auc:.4f}, "
            f"фичи из {cfg.ml_customer_features}"
        ),
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Важность признаков
# MAGIC Permutation importance честнее встроенной важности: она считается
# MAGIC на отложенной выборке и не завышает вклад признаков с высокой кардинальностью.

# COMMAND ----------

from sklearn.inspection import permutation_importance

result = permutation_importance(model, X_test, y_test, n_repeats=5, random_state=42, n_jobs=1)
importance = (
    pd.DataFrame({"feature": FEATURES, "importance": result.importances_mean})
    .sort_values("importance", ascending=False)
    .head(15)
)
display(spark.createDataFrame(importance))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ### Дальше
# MAGIC `63_batch_inference.py`

# Databricks notebook source
# MAGIC %md
# MAGIC # Part 6.3 — Батч-инференс
# MAGIC
# MAGIC Модель из реестра оборачивается в Spark UDF и применяется ко всей таблице
# MAGIC фич распределённо. Ключевые моменты:
# MAGIC
# MAGIC * ссылаемся на **алиас** (`models:/<catalog>.<schema>.<model>@champion`),
# MAGIC   а не на номер версии — смена чемпиона не требует правки кода;
# MAGIC * `mlflow.pyfunc.spark_udf` сам раскладывает модель по воркерам;
# MAGIC * рядом с оценкой пишем версию модели и дату — без этого невозможно
# MAGIC   разобраться, какой моделью посчитан конкретный скор.

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821
dbutils.widgets.text("model_alias", "champion")  # noqa: F821

# COMMAND ----------

import mlflow
from pyspark.sql import functions as F

from ecom.config import Config
from ecom.utils.logging import get_logger

log = get_logger("inference")
cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
alias = dbutils.widgets.get("model_alias")  # noqa: F821

mlflow.set_registry_uri("databricks-uc")
MODEL_URI = f"models:/{cfg.ml_churn_model}@{alias}"
print(f"модель: {MODEL_URI}")

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
model_version = client.get_model_version_by_alias(cfg.ml_churn_model, alias)
print(f"версия {model_version.version}, run {model_version.run_id}")

# COMMAND ----------

# MAGIC %md ## Скоринг

# COMMAND ----------

features = spark.table(cfg.ml_customer_features)  # noqa: F821

predict = mlflow.pyfunc.spark_udf(spark, MODEL_URI, result_type="double")  # noqa: F821
input_columns = predict.metadata.get_input_schema().input_names()

scored = (
    features.withColumn("churn_probability", predict(*[F.col(c) for c in input_columns]))
    .withColumn(
        "risk_band",
        F.when(F.col("churn_probability") >= 0.7, F.lit("high"))
        .when(F.col("churn_probability") >= 0.4, F.lit("medium"))
        .otherwise(F.lit("low")),
    )
    .withColumn("model_version", F.lit(model_version.version))
    .withColumn("scored_at", F.current_timestamp())
    .select(
        "customer_id",
        "as_of_date",
        "segment",
        "country",
        "recency_days",
        "orders_cnt",
        "total_spent",
        "churn_probability",
        "risk_band",
        "model_version",
        "scored_at",
    )
)

(
    scored.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.gold_churn_scores)
)
spark.sql(  # noqa: F821
    f"COMMENT ON TABLE {cfg.gold_churn_scores} IS "
    f"'Вероятность оттока по клиентам, посчитанная моделью {cfg.ml_churn_model}'"
)
log.info("записано %s строк", scored.count())

# COMMAND ----------

# MAGIC %md ## Распределение риска

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"""
        SELECT risk_band, segment,
               count(*) AS customers,
               round(avg(churn_probability), 3) AS avg_prob,
               round(sum(total_spent), 2)       AS revenue_at_risk
        FROM {cfg.gold_churn_scores}
        GROUP BY risk_band, segment
        ORDER BY risk_band, revenue_at_risk DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Кого спасать в первую очередь
# MAGIC Высокий риск сам по себе ничего не стоит — важно пересечение
# MAGIC «высокий риск × высокая ценность». Это и есть список для маркетинга.

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"""
        SELECT s.customer_id, s.segment, s.country,
               round(s.churn_probability, 3) AS churn_prob,
               round(s.total_spent, 2)       AS lifetime_value,
               r.segment                     AS rfm_segment
        FROM {cfg.gold_churn_scores} s
        LEFT JOIN {cfg.gold_customer_rfm} r USING (customer_id)
        WHERE s.risk_band = 'high'
        ORDER BY s.total_spent DESC
        LIMIT 50
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Мониторинг дрейфа (упрощённо)
# MAGIC Сравниваем распределение предсказаний с предыдущим запуском.
# MAGIC Резкий сдвиг средней вероятности — сигнал, что либо изменились данные,
# MAGIC либо модель пора переобучать.
# MAGIC
# MAGIC На платном воркспейсе то же самое делает Lakehouse Monitoring:
# MAGIC `CREATE MONITOR` на таблице со скорами считает дрейф автоматически.

# COMMAND ----------

HISTORY = cfg.table("ops", "churn_score_history")

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {HISTORY} (
      scored_at      TIMESTAMP,
      model_version  STRING,
      customers      BIGINT,
      avg_probability DOUBLE,
      high_risk_share DOUBLE
    ) COMMENT 'История распределения скоров — простейший мониторинг дрейфа'
    """
)

spark.sql(  # noqa: F821
    f"""
    INSERT INTO {HISTORY}
    SELECT
      max(scored_at), max(model_version), count(*),
      avg(churn_probability),
      sum(CASE WHEN risk_band = 'high' THEN 1 ELSE 0 END) / count(*)
    FROM {cfg.gold_churn_scores}
    """
)

display(spark.sql(f"SELECT * FROM {HISTORY} ORDER BY scored_at DESC LIMIT 10"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ### Дальше
# MAGIC `sql/queries/` + дашборд и Genie — см. `docs/part-06-analytics-ml.md`.

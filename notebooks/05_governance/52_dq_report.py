# Databricks notebook source
# MAGIC %md
# MAGIC # Part 5.3 — Отчёт по качеству данных
# MAGIC
# MAGIC Финальная задача ежедневной джобы. Делает три вещи:
# MAGIC
# MAGIC 1. Считает свежие метрики по Gold-витринам (то, чего нет в Silver-проверках):
# MAGIC    целостность ссылок, отсутствие «дыр» в календаре, вменяемость сумм.
# MAGIC 2. Сравнивает с предыдущим запуском — резкое изменение доли брака важнее
# MAGIC    её абсолютного значения.
# MAGIC 3. Отдаёт результат в `ops.dq_results` и падает, если нарушено
# MAGIC    блокирующее правило.
# MAGIC
# MAGIC Задача помечена `run_if: AT_LEAST_ONE_SUCCESS`, поэтому отчёт пишется даже
# MAGIC когда предыдущий шаг упал.

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821
dbutils.widgets.text("max_failure_rate_delta", "0.05")  # noqa: F821

# COMMAND ----------

import datetime as dt
import uuid

from pyspark.sql import functions as F

from ecom.config import Config
from ecom.quality.checks import DQ_RESULTS_DDL
from ecom.utils.logging import get_logger

log = get_logger("dq_report")
cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
threshold = float(dbutils.widgets.get("max_failure_rate_delta"))  # noqa: F821
RUN_ID = str(uuid.uuid4())
RUN_TS = dt.datetime.now()

spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821
spark.sql(DQ_RESULTS_DDL.format(table=cfg.ops_dq_results))  # noqa: F821

# COMMAND ----------

# MAGIC %md ## Проверки уровня витрин

# COMMAND ----------

GOLD_CHECKS = [
    (
        "gold_fct_orphan_orders",
        "fail",
        f"""
        SELECT count(*) FROM {cfg.gold_fct_order_items} f
        LEFT ANTI JOIN {cfg.silver_orders} o ON f.order_id = o.order_id
        """,
        "Позиции факта, для которых нет заказа в Silver",
    ),
    (
        "gold_fct_unknown_product",
        "warn",
        f"SELECT count(*) FROM {cfg.gold_fct_order_items} WHERE category IS NULL",
        "Товар не нашёлся в справочнике",
    ),
    (
        "gold_daily_sales_negative",
        "fail",
        f"SELECT count(*) FROM {cfg.gold_daily_sales} WHERE net_revenue < 0",
        "Отрицательная выручка в витрине",
    ),
    (
        "gold_date_gaps",
        "warn",
        f"""
        SELECT count(*) FROM (
          SELECT d.date_key
          FROM {cfg.gold_dim_date} d
          LEFT JOIN (SELECT DISTINCT order_date FROM {cfg.gold_daily_sales}) s
                 ON d.date_key = s.order_date
          WHERE s.order_date IS NULL
        )
        """,
        "Дни календаря без продаж — возможен пропуск загрузки",
    ),
    (
        "gold_rfm_covers_customers",
        "warn",
        f"""
        SELECT count(*) FROM {cfg.gold_dim_customer} c
        LEFT ANTI JOIN {cfg.gold_customer_rfm} r ON c.customer_id = r.customer_id
        """,
        "Клиенты без RFM-оценки (нормально для тех, кто ещё не покупал)",
    ),
]

rows_total = spark.table(cfg.gold_fct_order_items).count()  # noqa: F821

results = []
for name, severity, query, description in GOLD_CHECKS:
    failed = spark.sql(query).first()[0]  # noqa: F821
    results.append(
        (
            RUN_ID,
            RUN_TS,
            "gold",
            name,
            severity,
            description,
            rows_total,
            int(failed),
            (failed / rows_total) if rows_total else 0.0,
            failed == 0,
        )
    )
    log.info("%-28s failed=%s", name, failed)

metrics = spark.createDataFrame(  # noqa: F821
    results,
    schema=(
        "run_id string, run_ts timestamp, dataset string, rule_name string, severity string, "
        "expression string, rows_total bigint, rows_failed bigint, failure_rate double, passed boolean"
    ),
)
metrics.write.mode("append").saveAsTable(cfg.ops_dq_results)
display(metrics)  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Сравнение с прошлым запуском
# MAGIC Абсолютная доля брака мало о чём говорит — важна динамика.
# MAGIC Скачок больше порога означает, что что-то поменялось в источнике.

# COMMAND ----------

trend = spark.sql(  # noqa: F821
    f"""
    WITH ranked AS (
      SELECT
        dataset, rule_name, severity, failure_rate, run_ts,
        row_number() OVER (PARTITION BY dataset, rule_name ORDER BY run_ts DESC) AS rn
      FROM {cfg.ops_dq_results}
    )
    SELECT
      c.dataset, c.rule_name, c.severity,
      round(c.failure_rate, 4) AS current_rate,
      round(p.failure_rate, 4) AS previous_rate,
      round(c.failure_rate - p.failure_rate, 4) AS delta
    FROM ranked c
    JOIN ranked p ON c.dataset = p.dataset AND c.rule_name = p.rule_name AND p.rn = 2
    WHERE c.rn = 1
    ORDER BY abs(c.failure_rate - p.failure_rate) DESC
    """
)
display(trend)  # noqa: F821

# COMMAND ----------

regressions = trend.where(F.col("delta") > threshold).collect()
for row in regressions:
    log.warning(
        "деградация качества: %s.%s  %.4f → %.4f",
        row["dataset"],
        row["rule_name"],
        row["previous_rate"],
        row["current_rate"],
    )

# COMMAND ----------

# MAGIC %md ## Карантин: что именно отбраковано

# COMMAND ----------

for dataset in ["orders", "order_items", "clickstream"]:
    table = cfg.table("ops", f"quarantine_{dataset}")
    if spark.catalog.tableExists(table):  # noqa: F821
        print(f"\n=== {dataset} ===")
        spark.sql(  # noqa: F821
            f"""
            SELECT rule, count(*) AS rows
            FROM {table} LATERAL VIEW explode(_dq_failed_rules) t AS rule
            GROUP BY rule ORDER BY rows DESC
            """
        ).show(truncate=False)

# COMMAND ----------

# MAGIC %md ## Блокирующие нарушения — роняем задачу

# COMMAND ----------

blocking = metrics.where((F.col("severity") == "fail") & (~F.col("passed"))).collect()
if blocking:
    details = ", ".join(f"{r['rule_name']}={r['rows_failed']}" for r in blocking)
    raise RuntimeError(f"Блокирующие нарушения качества: {details}")

print("Качество в норме")

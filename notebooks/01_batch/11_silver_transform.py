# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1.2 — Silver: типизация, дедуп, DQ и SCD Type 2
# MAGIC
# MAGIC Здесь три идеи, которые отличают Silver от «просто скопировали и почистили»:
# MAGIC
# MAGIC 1. **Дедуп детерминирован.** `dropDuplicates()` без порядка выбирает случайную
# MAGIC    строку — при перезапуске результат может поменяться. Мы используем
# MAGIC    `row_number()` по `_ingested_at`.
# MAGIC 2. **Ничего не теряется молча.** Строки, не прошедшие правила, уезжают в
# MAGIC    `ops.quarantine_*` с перечнем нарушенных правил, а метрики — в `ops.dq_results`.
# MAGIC 3. **История клиентов — SCD Type 2.** Реализована одним MERGE (см. `ecom.transforms.scd`).
# MAGIC    В Part 2 то же самое делает `create_auto_cdc_flow` одной строкой — сравните.

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821

# COMMAND ----------

import uuid

from pyspark.sql import functions as F

from ecom.config import Config
from ecom.quality.checks import (
    DQ_RESULTS_DDL,
    assert_no_fail_violations,
    evaluate_rules,
    split_valid_invalid,
)
from ecom.transforms.scd import (
    create_scd2_table_sql,
    current_view_sql,
    dedupe_latest_sql,
    scd2_merge_sql,
)
from ecom.transforms.silver import (
    clean_clickstream,
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_products,
)
from ecom.utils.logging import get_logger

log = get_logger("silver")
cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
RUN_ID = str(uuid.uuid4())

spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821
spark.sql(DQ_RESULTS_DDL.format(table=cfg.ops_dq_results))  # noqa: F821

# COMMAND ----------

# MAGIC %md ## Общий конвейер: clean → DQ → split → write

# COMMAND ----------


def process(dataset: str, source_table: str, target_table: str, cleaner, cluster_by: str) -> dict:
    raw = spark.table(source_table)  # noqa: F821
    cleaned = cleaner(raw)

    metrics = evaluate_rules(cleaned, dataset, run_id=RUN_ID)
    metrics.write.mode("append").saveAsTable(cfg.ops_dq_results)
    assert_no_fail_violations(metrics)

    valid, invalid = split_valid_invalid(cleaned, dataset)

    (
        valid.write.mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    # Liquid clustering вместо партиционирования: Delta сама поддерживает раскладку,
    # и набор ключей можно менять без перезаписи таблицы.
    spark.sql(f"ALTER TABLE {target_table} CLUSTER BY ({cluster_by})")  # noqa: F821

    quarantine_table = cfg.table("ops", f"quarantine_{dataset}")
    invalid_cnt = invalid.count()
    if invalid_cnt:
        (
            invalid.withColumn("_run_id", F.lit(RUN_ID))
            .write.mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(quarantine_table)
        )

    stats = {"dataset": dataset, "valid": valid.count(), "quarantined": invalid_cnt}
    log.info("%s", stats)
    return stats


# COMMAND ----------

# MAGIC %md ## Orders, order_items, products, clickstream

# COMMAND ----------

stats = [
    process("orders", cfg.bronze_orders, cfg.silver_orders, clean_orders, "order_date,customer_id"),
    process(
        "order_items",
        cfg.bronze_order_items,
        cfg.silver_order_items,
        clean_order_items,
        "order_id,product_id",
    ),
    process(
        "clickstream",
        cfg.bronze_clickstream,
        cfg.silver_clickstream,
        clean_clickstream,
        "event_date,session_id",
    ),
]

# products: правил нет, пишем напрямую
(
    clean_products(spark.table(cfg.bronze_products))  # noqa: F821
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.silver_products)
)
display(spark.createDataFrame(stats))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Клиенты: SCD Type 2
# MAGIC
# MAGIC Вход — CDC-фид (`bronze.customers_raw`): полный снимок в первый день и дельты дальше.
# MAGIC Перед MERGE оставляем по одной, самой свежей, строке на `customer_id` —
# MAGIC иначе Delta справедливо ругается на несколько совпадений для одной целевой строки.

# COMMAND ----------

customers_clean = clean_customers(spark.table(cfg.bronze_customers))  # noqa: F821
customers_clean.createOrReplaceTempView("customers_clean_all")

spark.sql(create_scd2_table_sql(cfg.silver_customers_scd2))  # noqa: F821

latest = spark.sql(dedupe_latest_sql("customers_clean_all"))  # noqa: F821
latest.createOrReplaceTempView("customers_updates")

merge_sql = scd2_merge_sql(target=cfg.silver_customers_scd2, source="customers_updates")
print(merge_sql)

# COMMAND ----------

spark.sql(merge_sql)  # noqa: F821
spark.sql(current_view_sql(cfg.silver_customers_current, cfg.silver_customers_scd2))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ### Проверяем историю
# MAGIC Клиент, у которого сменился сегмент или город, должен иметь несколько версий:
# MAGIC ровно одну текущую (`is_current = true`, `__END_AT IS NULL`) и закрытые.

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"""
        WITH multi AS (
          SELECT customer_id
          FROM {cfg.silver_customers_scd2}
          GROUP BY customer_id HAVING count(*) > 1
          LIMIT 3
        )
        SELECT s.customer_id, s.segment, s.city, s.__START_AT, s.__END_AT, s.is_current
        FROM {cfg.silver_customers_scd2} s
        JOIN multi USING (customer_id)
        ORDER BY s.customer_id, s.__START_AT
        """
    )
)

# COMMAND ----------

# MAGIC %md ### Инвариант SCD2: ровно одна текущая версия на клиента

# COMMAND ----------

broken = spark.sql(  # noqa: F821
    f"""
    SELECT customer_id, count(*) AS current_versions
    FROM {cfg.silver_customers_scd2}
    WHERE is_current = true
    GROUP BY customer_id
    HAVING count(*) <> 1
    """
).count()
assert broken == 0, f"SCD2 нарушен для {broken} клиентов"
print("SCD2 OK")

# COMMAND ----------

# MAGIC %md ## Сводка по качеству за этот запуск

# COMMAND ----------

display(  # noqa: F821
    spark.table(cfg.ops_dq_results)  # noqa: F821
    .where(F.col("run_id") == RUN_ID)
    .select("dataset", "rule_name", "severity", "rows_total", "rows_failed", "failure_rate")
    .orderBy(F.col("failure_rate").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Дальше
# MAGIC `12_gold_marts.py`

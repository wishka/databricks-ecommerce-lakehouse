# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2.2 — LDP Silver: expectations вместо ручного разбора
# MAGIC
# MAGIC Три уровня expectations и когда какой брать:
# MAGIC
# MAGIC | Декоратор | Поведение | Когда |
# MAGIC |---|---|---|
# MAGIC | `@dp.expect_all` | строка проходит, нарушение считается в метрики | «предупреждение»: следим, но не режем |
# MAGIC | `@dp.expect_all_or_drop` | строка отбрасывается | битые данные, которые нельзя пускать в витрины |
# MAGIC | `@dp.expect_all_or_fail` | падает весь update пайплайна | нарушена целостность, считать дальше бессмысленно |
# MAGIC
# MAGIC Правила берутся из общего реестра `ecom.quality.rules` — того же самого,
# MAGIC который используется в batch-версии (Part 1). Пакет ставится в окружение
# MAGIC пайплайна через `environment.dependencies: --editable ${workspace.file_path}`
# MAGIC в `resources/pipelines/ecommerce_ldp.yml`.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ecom.quality.rules import as_expectations

ORDERS_FAIL = as_expectations("orders", "fail")
ORDERS_DROP = as_expectations("orders", "drop")
ORDERS_WARN = as_expectations("orders", "warn")

ITEMS_FAIL = as_expectations("order_items", "fail")
ITEMS_DROP = as_expectations("order_items", "drop")
ITEMS_WARN = as_expectations("order_items", "warn")

EVENTS_FAIL = as_expectations("clickstream", "fail")
EVENTS_DROP = as_expectations("clickstream", "drop")

VALID_STATUSES = ["created", "paid", "shipped", "delivered", "cancelled", "returned"]
REVENUE_STATUSES = ["paid", "shipped", "delivered"]

# COMMAND ----------


@dp.table(
    name="silver_orders",
    comment="Заказы: типизированы, дедуплицированы, провалидированы",
    table_properties={"quality": "silver"},
    cluster_by=["order_date", "customer_id"],
)
@dp.expect_all_or_fail(ORDERS_FAIL)
@dp.expect_all_or_drop(ORDERS_DROP)
@dp.expect_all(ORDERS_WARN)
def silver_orders():
    return (
        spark.readStream.table("bronze_orders")  # noqa: F821
        .withWatermark("_ingested_at", "1 day")
        # дедуп по ключу в пределах окна watermark: ретраи продюсера приходят
        # близко по времени, поэтому суток с запасом хватает
        .dropDuplicatesWithinWatermark(["order_id"])
        .select(
            F.trim("order_id").alias("order_id"),
            F.nullif(F.trim("customer_id"), F.lit("")).alias("customer_id"),
            F.to_timestamp("order_ts").alias("order_ts"),
            F.lower(F.trim("status")).alias("status_raw"),
            F.lower(F.trim("channel")).alias("channel"),
            F.upper(F.trim("currency")).alias("currency"),
            F.upper(F.trim("shipping_country")).alias("shipping_country"),
            F.col("discount_pct").cast(T.DoubleType()).alias("discount_pct"),
            F.col("_ingested_at"),
            F.col("_source_file"),
        )
        .withColumn(
            "status",
            F.when(F.col("status_raw").isin(VALID_STATUSES), F.col("status_raw")).otherwise(
                F.lit("unknown")
            ),
        )
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("is_revenue", F.col("status").isin(REVENUE_STATUSES))
        .drop("status_raw")
    )


# COMMAND ----------


@dp.table(
    name="silver_order_items",
    comment="Позиции заказов с рассчитанной суммой строки",
    table_properties={"quality": "silver"},
    cluster_by=["order_id", "product_id"],
)
@dp.expect_all_or_fail(ITEMS_FAIL)
@dp.expect_all_or_drop(ITEMS_DROP)
@dp.expect_all(ITEMS_WARN)
def silver_order_items():
    return (
        spark.readStream.table("bronze_order_items")  # noqa: F821
        .withWatermark("_ingested_at", "1 day")
        .dropDuplicatesWithinWatermark(["order_item_id"])
        .select(
            F.trim("order_item_id").alias("order_item_id"),
            F.trim("order_id").alias("order_id"),
            F.trim("product_id").alias("product_id"),
            F.col("quantity").cast(T.IntegerType()).alias("quantity"),
            F.col("unit_price").cast(T.DecimalType(12, 2)).alias("unit_price"),
            F.col("_ingested_at"),
        )
        .withColumn(
            "line_amount",
            (F.col("quantity") * F.col("unit_price")).cast(T.DecimalType(14, 2)),
        )
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## Справочник товаров — materialized view
# MAGIC Он маленький и меняется целиком, поэтому streaming table здесь избыточна:
# MAGIC MV просто пересчитывается на каждом апдейте.


# COMMAND ----------


@dp.materialized_view(
    name="silver_products",
    comment="Актуальный справочник товаров (последняя версия по product_id)",
    table_properties={"quality": "silver"},
)
def silver_products():
    from pyspark.sql import Window

    w = Window.partitionBy("product_id").orderBy(F.col("updated_at").desc())
    return (
        spark.read.table("bronze_products")  # noqa: F821
        .select(
            F.trim("product_id").alias("product_id"),
            F.trim("product_name").alias("product_name"),
            F.initcap(F.trim("category")).alias("category"),
            F.initcap(F.trim("subcategory")).alias("subcategory"),
            F.trim("brand").alias("brand"),
            F.col("list_price").cast(T.DecimalType(12, 2)).alias("list_price"),
            F.col("is_active").cast(T.BooleanType()).alias("is_active"),
            F.to_timestamp("updated_at").alias("updated_at"),
        )
        .withColumn("_rn", F.row_number().over(w))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


# COMMAND ----------


@dp.table(
    name="silver_clickstream",
    comment="События клик-стрима с производными полями",
    table_properties={"quality": "silver"},
    cluster_by=["event_date", "session_id"],
)
@dp.expect_all_or_fail(EVENTS_FAIL)
@dp.expect_all_or_drop(EVENTS_DROP)
def silver_clickstream():
    return (
        spark.readStream.table("bronze_clickstream")  # noqa: F821
        .withWatermark("_ingested_at", "1 day")
        .dropDuplicatesWithinWatermark(["event_id"])
        .select(
            F.trim("event_id").alias("event_id"),
            F.trim("session_id").alias("session_id"),
            F.nullif(F.trim("customer_id"), F.lit("")).alias("customer_id"),
            F.to_timestamp("event_ts").alias("event_ts"),
            F.lower(F.trim("event_type")).alias("event_type"),
            F.nullif(F.trim("product_id"), F.lit("")).alias("product_id"),
            F.lower(F.trim("device")).alias("device"),
        )
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("is_conversion", F.col("event_type") == F.lit("checkout"))
    )

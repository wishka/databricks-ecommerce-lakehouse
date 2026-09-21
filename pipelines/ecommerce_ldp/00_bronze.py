# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2.1 — LDP Bronze: streaming tables поверх Auto Loader
# MAGIC
# MAGIC Lakeflow Spark Declarative Pipelines: вместо «прочитать → записать → не забыть
# MAGIC чекпоинт» мы объявляем, **что** должно быть в таблице, а движок сам решает,
# MAGIC как её посчитать, в каком порядке и что пересчитывать.
# MAGIC
# MAGIC * `@dp.table` поверх `readStream` → **streaming table**: инкрементально,
# MAGIC   каждая строка обрабатывается один раз, чекпоинт ведёт движок.
# MAGIC * `@dp.materialized_view` → результат запроса, который пересчитывается
# MAGIC   (по возможности инкрементально) при каждом запуске.
# MAGIC
# MAGIC Целевые каталог и схему задаёт конфигурация пайплайна
# MAGIC (`resources/pipelines/ecommerce_ldp.yml`), в коде их писать не нужно.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Параметры пайплайна: заданы в spec как `configuration`.
CATALOG = spark.conf.get("ecom.catalog", "ecom_dev")  # noqa: F821
LANDING = f"/Volumes/{CATALOG}/raw/landing"

COMMON_CLOUDFILES = {
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.schemaEvolutionMode": "addNewColumns",
    "rescuedDataColumn": "_rescued_data",
}


def _autoload(dataset: str, fmt: str = "json", extra: dict | None = None):
    """Один источник — один Auto Loader. schemaLocation здесь не нужен:
    в LDP движок хранит схему и чекпоинт сам."""
    options = {"cloudFiles.format": fmt, **COMMON_CLOUDFILES, **(extra or {})}
    return (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .options(**options)
        .load(f"{LANDING}/{dataset}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


# COMMAND ----------


@dp.table(
    name="bronze_orders",
    comment="Сырые заказы из landing (Auto Loader)",
    table_properties={"quality": "bronze", "delta.enableChangeDataFeed": "true"},
)
def bronze_orders():
    return _autoload("orders")


@dp.table(
    name="bronze_order_items",
    comment="Сырые позиции заказов",
    table_properties={"quality": "bronze"},
)
def bronze_order_items():
    return _autoload("order_items")


@dp.table(
    name="bronze_products",
    comment="Сырой справочник товаров",
    table_properties={"quality": "bronze"},
)
def bronze_products():
    return _autoload("products")


@dp.table(
    name="bronze_customers",
    comment="CDC-фид клиентов: полный снимок в первый день, далее дельты",
    table_properties={"quality": "bronze"},
)
def bronze_customers():
    return _autoload(
        "customers",
        fmt="csv",
        extra={"header": "true", "cloudFiles.inferColumnTypes": "false"},
    )


@dp.table(
    name="bronze_clickstream",
    comment="Сырые события клик-стрима",
    table_properties={"quality": "bronze"},
)
def bronze_clickstream():
    return _autoload("clickstream")

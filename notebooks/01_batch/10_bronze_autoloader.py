# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1.1 — Bronze: приём файлов через Auto Loader
# MAGIC
# MAGIC **Auto Loader** (`format("cloudFiles")`) — инкрементальный приём файлов:
# MAGIC он сам ведёт реестр уже обработанных файлов, поэтому повторный запуск
# MAGIC не перечитывает всё заново. Это главное отличие от `spark.read.json(path)`.
# MAGIC
# MAGIC Ключевые опции, которые здесь используются:
# MAGIC
# MAGIC | Опция | Зачем |
# MAGIC |---|---|
# MAGIC | `cloudFiles.schemaLocation` | где хранится выведенная схема и её эволюция |
# MAGIC | `cloudFiles.inferColumnTypes` | иначе всё придёт строками |
# MAGIC | `cloudFiles.schemaEvolutionMode=addNewColumns` | новая колонка в источнике → стрим падает один раз и подхватывает её |
# MAGIC | `rescuedDataColumn` | всё, что не влезло в схему, не теряется, а едет в `_rescued_data` |
# MAGIC | `cloudFiles.maxFilesPerTrigger` | контролируем размер микробатча |
# MAGIC
# MAGIC Триггер — `availableNow=True`: стрим забирает всё накопившееся и останавливается.
# MAGIC Это «батч поверх стриминга»: идеально для расписания раз в час/день,
# MAGIC и дешевле, чем всегда работающий стрим (важно для Free Edition).

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821
dbutils.widgets.dropdown("full_refresh", "false", ["true", "false"])  # noqa: F821

# COMMAND ----------

from pyspark.sql import functions as F

from ecom.config import Config
from ecom.utils.logging import get_logger

log = get_logger("bronze")
cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
full_refresh = dbutils.widgets.get("full_refresh") == "true"  # noqa: F821

spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Универсальная функция приёма
# MAGIC Одна функция на все датасеты: разница только в формате и опциях.

# COMMAND ----------


def ingest_to_bronze(
    dataset: str,
    target_table: str,
    file_format: str = "json",
    reader_options: dict | None = None,
    partition_column: str | None = None,
) -> int:
    """Auto Loader: landing/<dataset> → bronze-таблица.

    Возвращает число строк в целевой таблице после записи.
    """
    source_path = cfg.landing_path(dataset)
    schema_location = cfg.schema_location(f"bronze_{dataset}")
    checkpoint = cfg.checkpoint_path(f"bronze_{dataset}")

    if full_refresh:
        dbutils.fs.rm(checkpoint, recurse=True)  # noqa: F821
        dbutils.fs.rm(schema_location, recurse=True)  # noqa: F821
        spark.sql(f"DROP TABLE IF EXISTS {target_table}")  # noqa: F821

    options = {
        "cloudFiles.format": file_format,
        "cloudFiles.schemaLocation": schema_location,
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "cloudFiles.maxFilesPerTrigger": "50",
        "rescuedDataColumn": "_rescued_data",
        **(reader_options or {}),
    }

    stream = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .options(**options)
        .load(source_path)
        # технические поля: без них потом невозможно расследовать инцидент
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_file_modified_at", F.col("_metadata.file_modification_time"))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_batch_id", F.lit(spark.conf.get("spark.databricks.job.runId", "manual")))  # noqa: F821
    )

    writer = (
        stream.writeStream.option("checkpointLocation", checkpoint)
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
    )
    if partition_column:
        writer = writer.partitionBy(partition_column)

    query = writer.toTable(target_table)
    query.awaitTermination()

    count = spark.table(target_table).count()  # noqa: F821
    log.info("%-18s → %s (%s строк)", dataset, target_table, count)
    return count


# COMMAND ----------

# MAGIC %md
# MAGIC ## Приём всех датасетов
# MAGIC `customers` — CSV, поэтому нужен `header`. Остальное — JSON Lines.

# COMMAND ----------

results = {
    "orders": ingest_to_bronze("orders", cfg.bronze_orders),
    "order_items": ingest_to_bronze("order_items", cfg.bronze_order_items),
    "products": ingest_to_bronze("products", cfg.bronze_products),
    "customers": ingest_to_bronze(
        "customers",
        cfg.bronze_customers,
        file_format="csv",
        reader_options={"header": "true", "cloudFiles.inferColumnTypes": "false"},
    ),
    "clickstream": ingest_to_bronze("clickstream", cfg.bronze_clickstream),
}
results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Что такое `_rescued_data`
# MAGIC Если в файле пришло поле, которого нет в схеме, или значение не приводится
# MAGIC к выведенному типу — оно попадает сюда, а не теряется и не роняет приём.
# MAGIC Пустой `_rescued_data` — хороший знак; непустой — повод посмотреть источник.

# COMMAND ----------

display(  # noqa: F821
    spark.table(cfg.bronze_orders)  # noqa: F821
    .where(F.col("_rescued_data").isNotNull())
    .select("order_id", "_source_file", "_rescued_data")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md ## Комментарии и свойства таблиц
# MAGIC Комментарий на таблице — это не формальность: он виден в Catalog Explorer
# MAGIC и в Genie, и напрямую влияет на качество ответов natural-language BI (Part 6).

# COMMAND ----------

COMMENTS = {
    cfg.bronze_orders: "Сырые заказы из landing-зоны, как есть + технические поля приёма",
    cfg.bronze_order_items: "Сырые позиции заказов",
    cfg.bronze_products: "Сырой справочник товаров",
    cfg.bronze_customers: "Сырые снимки клиентов (CDC-фид: первый день — полный, далее дельты)",
    cfg.bronze_clickstream: "Сырые события клик-стрима",
}
for table, comment in COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")  # noqa: F821
    spark.sql(  # noqa: F821
        f"ALTER TABLE {table} SET TBLPROPERTIES ("
        "  delta.enableChangeDataFeed = true,"
        "  'ecom.layer' = 'bronze'"
        ")"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Проверка идемпотентности
# MAGIC Запустите ноутбук второй раз без новых файлов: счётчики не изменятся,
# MAGIC потому что Auto Loader помнит обработанные файлы в чекпоинте.
# MAGIC
# MAGIC ### Дальше
# MAGIC `11_silver_transform.py`

# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3.2 — Structured Streaming: окна, watermark, foreachBatch
# MAGIC
# MAGIC Четыре вещи, которые надо уметь объяснить:
# MAGIC
# MAGIC 1. **Event time ≠ processing time.** Агрегируем по времени события,
# MAGIC    а не по времени прихода файла.
# MAGIC 2. **Watermark** — обещание движку: «событий старше X мы больше не ждём».
# MAGIC    Без него состояние оконных агрегатов растёт бесконечно.
# MAGIC 3. **Режимы вывода.** `append` отдаёт окно только после закрытия водяным
# MAGIC    знаком; `update` отдаёт промежуточные результаты. В Delta-таблицу с
# MAGIC    агрегатами обычно пишут через `foreachBatch` + `MERGE`.
# MAGIC 4. **Чекпоинт** — это состояние запроса. Удалили чекпоинт — стрим
# MAGIC    перечитает всё заново; поменяли схему агрегата — старый чекпоинт
# MAGIC    несовместим.

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821
dbutils.widgets.dropdown("mode", "available_now", ["available_now", "continuous"])  # noqa: F821
dbutils.widgets.text("run_minutes", "5")  # noqa: F821

# COMMAND ----------

from pyspark.sql import functions as F

from ecom.config import Config
from ecom.utils.logging import get_logger

log = get_logger("streaming")
cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
mode = dbutils.widgets.get("mode")  # noqa: F821
run_minutes = int(dbutils.widgets.get("run_minutes"))  # noqa: F821

spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821

SOURCE = cfg.landing_path("clickstream_live")
TARGET = cfg.gold_sessions_5min
CHECKPOINT = cfg.checkpoint_path("sessions_5min")

# COMMAND ----------

# MAGIC %md ## Целевая таблица
# MAGIC Создаём заранее: `foreachBatch` + `MERGE` требует существующей таблицы.

# COMMAND ----------

spark.sql(  # noqa: F821
    f"""
    CREATE TABLE IF NOT EXISTS {TARGET} (
      window_start     TIMESTAMP NOT NULL,
      window_end       TIMESTAMP NOT NULL,
      device           STRING    NOT NULL,
      events_cnt       BIGINT,
      sessions_cnt     BIGINT,
      users_cnt        BIGINT,
      add_to_cart_cnt  BIGINT,
      checkout_cnt     BIGINT,
      conversion_rate  DOUBLE,
      updated_at       TIMESTAMP
    )
    USING DELTA
    COMMENT 'Активность на сайте пятиминутными окнами (near-real-time)'
    CLUSTER BY (window_start, device)
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Чтение потока
# MAGIC Auto Loader и здесь: он инкрементально видит новые файлы продюсера.

# COMMAND ----------

events = (
    spark.readStream.format("cloudFiles")  # noqa: F821
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", cfg.schema_location("clickstream_live"))
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.maxFilesPerTrigger", "20")
    .load(SOURCE)
    .withColumn("event_ts", F.to_timestamp("event_ts"))
    .withColumn("event_type", F.lower("event_type"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Дедупликация в потоке
# MAGIC `dropDuplicatesWithinWatermark` хранит ключи только в пределах watermark,
# MAGIC поэтому состояние ограничено. Обычный `dropDuplicates` на стриме копил бы
# MAGIC все ключи за всё время.

# COMMAND ----------

deduped = events.withWatermark("event_ts", "15 minutes").dropDuplicatesWithinWatermark(["event_id"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Оконная агрегация
# MAGIC Tumbling-окно 5 минут. Watermark 15 минут — значит опоздавшие до 15 минут
# MAGIC события ещё попадут в своё окно, более старые будут отброшены.

# COMMAND ----------

windowed = (
    deduped.groupBy(F.window("event_ts", "5 minutes"), F.col("device"))
    .agg(
        F.count("*").alias("events_cnt"),
        F.approx_count_distinct("session_id").alias("sessions_cnt"),
        F.approx_count_distinct("customer_id").alias("users_cnt"),
        F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_cnt"),
        F.sum(F.when(F.col("event_type") == "checkout", 1).otherwise(0)).alias("checkout_cnt"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "device",
        "events_cnt",
        "sessions_cnt",
        "users_cnt",
        "add_to_cart_cnt",
        "checkout_cnt",
    )
    .withColumn(
        "conversion_rate",
        F.col("checkout_cnt") / F.nullif(F.col("sessions_cnt"), F.lit(0)),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Запись через foreachBatch + MERGE
# MAGIC Почему не `writeStream.outputMode("update").toTable(...)`: с `update`
# MAGIC Delta-таблица не поддерживает апдейт строк напрямую. `foreachBatch` даёт
# MAGIC обычный батчевый DataFrame — с ним можно всё, включая MERGE и запись
# MAGIC в несколько мест сразу.
# MAGIC
# MAGIC `batch_id` в сигнатуре нужен для идемпотентности: при рестарте один и тот же
# MAGIC батч может прийти повторно, а MERGE по ключу окна перезапишет его теми же
# MAGIC значениями.


# COMMAND ----------


def upsert_window(batch_df, batch_id: int) -> None:
    if batch_df.isEmpty():
        return
    (
        batch_df.withColumn("updated_at", F.current_timestamp())
        .createOrReplaceTempView("window_updates")
    )
    batch_df.sparkSession.sql(
        f"""
        MERGE INTO {TARGET} AS t
        USING window_updates AS s
        ON t.window_start = s.window_start AND t.device = s.device
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    log.info("batch %s: окон записано %s", batch_id, batch_df.count())


# COMMAND ----------

writer = (
    windowed.writeStream.outputMode("update")
    .foreachBatch(upsert_window)
    .option("checkpointLocation", CHECKPOINT)
    .queryName("sessions_5min")
)

if mode == "available_now":
    query = writer.trigger(availableNow=True).start()
    query.awaitTermination()
else:
    query = writer.trigger(processingTime="30 seconds").start()
    query.awaitTermination(timeout=run_minutes * 60)
    query.stop()

print("статус:", query.lastProgress)

# COMMAND ----------

# MAGIC %md ## Что получилось

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"""
        SELECT window_start, device, events_cnt, sessions_cnt,
               round(conversion_rate, 3) AS conv_rate
        FROM {TARGET}
        ORDER BY window_start DESC, device
        LIMIT 40
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stream-static join: обогащение справочником
# MAGIC Статическая сторона перечитывается на каждом микробатче — то есть
# MAGIC изменения справочника подхватываются автоматически. Обратное неверно:
# MAGIC уже записанные строки не пересчитываются.

# COMMAND ----------

products = spark.table(cfg.silver_products).select("product_id", "category", "brand")  # noqa: F821

enriched = (
    spark.readStream.format("cloudFiles")  # noqa: F821
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", cfg.schema_location("clickstream_live_enriched"))
    .option("cloudFiles.inferColumnTypes", "true")
    .load(SOURCE)
    .join(F.broadcast(products), on="product_id", how="left")
    .withColumn("event_ts", F.to_timestamp("event_ts"))
)

enriched_query = (
    enriched.writeStream.option(
        "checkpointLocation", cfg.checkpoint_path("clickstream_live_enriched")
    )
    .trigger(availableNow=True)
    .toTable(cfg.table("silver", "clickstream_live_enriched"))
)
enriched_query.awaitTermination()
display(spark.table(cfg.table("silver", "clickstream_live_enriched")).limit(10))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Мониторинг стрима
# MAGIC `lastProgress` показывает `inputRowsPerSecond`, `processedRowsPerSecond`
# MAGIC и, главное, `numRowsDroppedByWatermark` — если это число растёт,
# MAGIC watermark слишком агрессивный и вы теряете опоздавшие данные.
# MAGIC
# MAGIC В проде вместо ручного разбора вешают `StreamingQueryListener`,
# MAGIC который шлёт метрики в таблицу:
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.streaming import StreamingQueryListener
# MAGIC
# MAGIC class MetricsListener(StreamingQueryListener):
# MAGIC     def onQueryProgress(self, event):
# MAGIC         p = event.progress
# MAGIC         spark.createDataFrame([(p.id, p.batchId, p.numInputRows)],
# MAGIC                               "query_id string, batch_id long, rows long") \
# MAGIC              .write.mode("append").saveAsTable("ops.stream_metrics")
# MAGIC
# MAGIC spark.streams.addListener(MetricsListener())
# MAGIC ```

# COMMAND ----------

for q in spark.streams.active:  # noqa: F821
    print(q.name, q.lastProgress.get("numInputRows"), q.lastProgress.get("durationMs"))
    q.stop()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Дальше
# MAGIC Part 4 — `databricks.yml` и джобы.

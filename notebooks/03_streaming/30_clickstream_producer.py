# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3.1 — Продюсер событий
# MAGIC
# MAGIC Чтобы тренировать стриминг, нужен источник, который **дописывает** файлы,
# MAGIC пока запрос работает. Этот ноутбук раз в N секунд подкладывает новую пачку
# MAGIC событий в `landing/clickstream_live/`.
# MAGIC
# MAGIC Запускать параллельно с `31_streaming_aggregations.py` (в соседней вкладке
# MAGIC или отдельной задачей джобы).
# MAGIC
# MAGIC Специально добавляем опоздавшие события — на них проверяется watermark.

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821
dbutils.widgets.text("batches", "12")  # noqa: F821
dbutils.widgets.text("interval_sec", "20")  # noqa: F821
dbutils.widgets.text("events_per_batch", "500")  # noqa: F821

# COMMAND ----------

import datetime as dt
import random
import time

from ecom.config import Config
from ecom.datagen.generator import generate_clickstream, generate_customers, generate_products
from ecom.datagen.writer import ensure_dir, write_jsonl
from ecom.utils.logging import get_logger

log = get_logger("producer")
cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
batches = int(dbutils.widgets.get("batches"))  # noqa: F821
interval = int(dbutils.widgets.get("interval_sec"))  # noqa: F821
per_batch = int(dbutils.widgets.get("events_per_batch"))  # noqa: F821

LIVE_PATH = cfg.landing_path("clickstream_live")
ensure_dir(LIVE_PATH)
print(f"пишу в {LIVE_PATH}")

# COMMAND ----------

customers = generate_customers(n=500, seed=99)
products = generate_products(n=100, seed=99)

for batch in range(batches):
    now = dt.datetime.now()
    events = generate_clickstream(
        now.date(), customers=customers, products=products, n_events=per_batch, seed=batch
    )

    # приводим event_ts к «сейчас», иначе стрим увидит только прошлое
    for event in events:
        shift = random.randint(0, 120)
        # 10% событий приходят с опозданием на 5–20 минут — проверка watermark
        if random.random() < 0.10:
            shift += random.randint(300, 1200)
        event["event_ts"] = (now - dt.timedelta(seconds=shift)).isoformat(timespec="seconds")
        event["ingest_ts"] = now.isoformat(timespec="seconds")

    path = f"{LIVE_PATH}/events_{now:%Y%m%d_%H%M%S}_{batch:03d}.json"
    write_jsonl(path, events)
    log.info("batch %s/%s → %s событий", batch + 1, batches, len(events))

    if batch < batches - 1:
        time.sleep(interval)

log.info("продюсер завершён")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Совет
# MAGIC Для непрерывного демо поставьте `batches=1000` и `interval_sec=30` —
# MAGIC и запустите этот ноутбук как отдельную задачу в джобе Part 4.

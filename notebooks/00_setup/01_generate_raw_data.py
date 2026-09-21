# Databricks notebook source
# MAGIC %md
# MAGIC # Part 0 — Генерация исходных данных в Volume
# MAGIC
# MAGIC Кладём в `/Volumes/<catalog>/raw/landing/` файлы за несколько дней:
# MAGIC
# MAGIC | Датасет | Формат | Зачем именно так |
# MAGIC |---|---|---|
# MAGIC | `orders/` | JSON Lines, по файлу на день | Auto Loader с inferColumnTypes |
# MAGIC | `order_items/` | JSON Lines | связь 1:N с заказом |
# MAGIC | `customers/` | CSV с заголовком | другой формат + CDC-снимки для SCD2 |
# MAGIC | `products/` | JSON Lines | маленький справочник → broadcast join |
# MAGIC | `clickstream/` | JSON Lines, мелкими чанками | несколько микробатчей для стрима |
# MAGIC
# MAGIC Специально закладываем дубли, NULL-и, отрицательные количества и опоздавшие
# MAGIC события — на них дальше тренируются дедуп, DQ и watermark.

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821
dbutils.widgets.text("start_date", "2026-08-01")  # noqa: F821
dbutils.widgets.text("days", "21")  # noqa: F821
dbutils.widgets.text("orders_per_day", "800")  # noqa: F821
dbutils.widgets.dropdown("reset", "false", ["true", "false"])  # noqa: F821

# COMMAND ----------

import datetime as dt
import shutil

from ecom.config import Config
from ecom.datagen.generator import (
    evolve_customers,
    generate_clickstream,
    generate_customers,
    generate_day,
    generate_products,
)
from ecom.datagen.writer import ensure_dir, write_chunks, write_csv, write_jsonl
from ecom.utils.logging import get_logger

log = get_logger("datagen")

cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
start_date = dt.date.fromisoformat(dbutils.widgets.get("start_date"))  # noqa: F821
days = int(dbutils.widgets.get("days"))  # noqa: F821
orders_per_day = int(dbutils.widgets.get("orders_per_day"))  # noqa: F821
reset = dbutils.widgets.get("reset") == "true"  # noqa: F821

ROOT = cfg.volume_root()
print(f"landing root: {ROOT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Сброс (осторожно)
# MAGIC Удаляет ВСЁ содержимое landing, включая чекпоинты стримов.
# MAGIC Нужен, когда хотите перегенерировать данные с нуля.

# COMMAND ----------

if reset:
    shutil.rmtree(ROOT, ignore_errors=True)
    log.warning("landing очищен")
ensure_dir(ROOT)

# COMMAND ----------

# MAGIC %md ## Справочники

# COMMAND ----------

products = generate_products(n=300, seed=42, as_of=start_date)
customers = generate_customers(n=2000, seed=7, as_of=start_date)

write_jsonl(f"{ROOT}/products/products_{start_date:%Y%m%d}.json", products)
write_csv(f"{ROOT}/customers/customers_{start_date:%Y%m%d}.csv", customers)

log.info("products=%s customers=%s", len(products), len(customers))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Транзакции по дням
# MAGIC Плюс ежедневный CDC-фид по клиентам: только изменившиеся записи.
# MAGIC Из него в Part 1 строится SCD2 через MERGE, а в Part 2 — через AUTO CDC.

# COMMAND ----------

total_orders = total_items = total_events = total_cdc = 0

for offset in range(days):
    day = start_date + dt.timedelta(days=offset)

    orders, items = generate_day(
        day,
        customers=customers,
        products=products,
        orders_per_day=orders_per_day,
        seed=offset,
    )
    write_jsonl(f"{ROOT}/orders/orders_{day:%Y%m%d}.json", orders)
    write_jsonl(f"{ROOT}/order_items/order_items_{day:%Y%m%d}.json", items)

    events = generate_clickstream(day, customers=customers, products=products, n_events=4000)
    write_chunks(f"{ROOT}/clickstream", events, prefix=f"events_{day:%Y%m%d}", chunk_size=800)

    if offset > 0:  # первый день — полный снимок, дальше только дельты
        changed = evolve_customers(customers, as_of=day, change_ratio=0.03)
        if changed:
            write_csv(f"{ROOT}/customers/customers_{day:%Y%m%d}.csv", changed)
            total_cdc += len(changed)
            # синхронизируем «источник», чтобы следующие дельты шли от новой версии
            by_id = {c["customer_id"]: c for c in changed}
            customers = [by_id.get(c["customer_id"], c) for c in customers]

    total_orders += len(orders)
    total_items += len(items)
    total_events += len(events)

log.info(
    "orders=%s items=%s events=%s customer_cdc_rows=%s",
    total_orders,
    total_items,
    total_events,
    total_cdc,
)

# COMMAND ----------

# MAGIC %md ## Что получилось

# COMMAND ----------

for folder in ["orders", "order_items", "customers", "products", "clickstream"]:
    files = dbutils.fs.ls(f"{ROOT}/{folder}")  # noqa: F821
    size_mb = sum(f.size for f in files) / 1024 / 1024
    print(f"{folder:<14} files={len(files):>4}  size={size_mb:6.1f} MB")

# COMMAND ----------

# MAGIC %md
# MAGIC Подглядим в один файл — полезно перед настройкой Auto Loader.

# COMMAND ----------

display(spark.read.json(f"{ROOT}/orders").limit(5))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ### Дальше
# MAGIC Part 1 — `notebooks/01_batch/10_bronze_autoloader.py`

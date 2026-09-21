# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1.3 — Gold: звезда и аналитические витрины
# MAGIC
# MAGIC Строим:
# MAGIC * `dim_date`, `dim_customer`, `dim_product` — измерения;
# MAGIC * `fct_order_items` — факт на уровне позиции заказа;
# MAGIC * `daily_sales` — продажи по дню/каналу/категории + скользящее среднее;
# MAGIC * `customer_rfm` — RFM-сегментация;
# MAGIC * `product_performance` — топ товаров внутри категории.
# MAGIC
# MAGIC Вся логика — в `ecom.transforms.gold`, здесь только оркестрация и запись.
# MAGIC Это принципиально: тестами покрывается модуль, а не ноутбук.

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821

# COMMAND ----------

from pyspark.sql import functions as F

from ecom.config import Config
from ecom.transforms.gold import (
    build_customer_rfm,
    build_daily_sales,
    build_dim_customer,
    build_dim_date,
    build_dim_product,
    build_fct_order_items,
    build_product_performance,
)
from ecom.utils.logging import get_logger

log = get_logger("gold")
cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821

orders = spark.table(cfg.silver_orders)  # noqa: F821
order_items = spark.table(cfg.silver_order_items)  # noqa: F821
products = spark.table(cfg.silver_products)  # noqa: F821
customers = spark.table(cfg.silver_customers_current)  # noqa: F821

# COMMAND ----------

# MAGIC %md ## Измерения

# COMMAND ----------

bounds = orders.agg(F.min("order_date").alias("lo"), F.max("order_date").alias("hi")).first()

(
    build_dim_date(spark, bounds["lo"], bounds["hi"])  # noqa: F821
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.gold_dim_date)
)
(
    build_dim_customer(customers)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.gold_dim_customer)
)
(
    build_dim_product(products)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.gold_dim_product)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Факт
# MAGIC `products` мал — join помечен `broadcast`, чтобы не было shuffle.
# MAGIC Посмотрите план: `BroadcastHashJoin` вместо `SortMergeJoin`.

# COMMAND ----------

fct = build_fct_order_items(orders, order_items, products)
fct.explain("formatted")

# COMMAND ----------

(
    fct.write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.gold_fct_order_items)
)
spark.sql(  # noqa: F821
    f"ALTER TABLE {cfg.gold_fct_order_items} CLUSTER BY (order_date, category)"
)
spark.sql(f"OPTIMIZE {cfg.gold_fct_order_items}")  # noqa: F821

# COMMAND ----------

# MAGIC %md ## Витрины

# COMMAND ----------

fct_tbl = spark.table(cfg.gold_fct_order_items)  # noqa: F821

(
    build_daily_sales(fct_tbl)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.gold_daily_sales)
)
(
    build_customer_rfm(fct_tbl)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.gold_customer_rfm)
)
(
    build_product_performance(fct_tbl)
    .write.mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(cfg.table("gold", "product_performance"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ограничения и метаданные
# MAGIC `PRIMARY KEY` / `FOREIGN KEY` в UC — informational: движок их не проверяет,
# MAGIC но их видят BI-инструменты и Genie, и они строят по ним связи.
# MAGIC А вот `CHECK`-constraint Delta проверяет на запись по-настоящему.

# COMMAND ----------

spark.sql(f"ALTER TABLE {cfg.gold_dim_customer} ALTER COLUMN customer_sk SET NOT NULL")  # noqa: F821
spark.sql(f"ALTER TABLE {cfg.gold_dim_product} ALTER COLUMN product_sk SET NOT NULL")  # noqa: F821
spark.sql(f"ALTER TABLE {cfg.gold_dim_date} ALTER COLUMN date_key SET NOT NULL")  # noqa: F821

for ddl in [
    f"ALTER TABLE {cfg.gold_dim_customer} ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)",
    f"ALTER TABLE {cfg.gold_dim_product}  ADD CONSTRAINT pk_dim_product  PRIMARY KEY (product_sk)",
    f"ALTER TABLE {cfg.gold_dim_date}     ADD CONSTRAINT pk_dim_date     PRIMARY KEY (date_key)",
    f"ALTER TABLE {cfg.gold_fct_order_items} ADD CONSTRAINT chk_qty CHECK (quantity > 0)",
]:
    try:
        spark.sql(ddl)  # noqa: F821
    except Exception as exc:  # constraint уже существует — это нормально при переgone
        log.warning("skip: %s (%s)", ddl.split(" ADD ")[-1], type(exc).__name__)

# COMMAND ----------

COMMENTS = {
    cfg.gold_daily_sales: "Продажи по дате, каналу и категории: заказы, выручка нетто, AOV, 7-дневное среднее",
    cfg.gold_customer_rfm: "RFM-сегментация клиентов: recency/frequency/monetary и итоговый сегмент",
    cfg.gold_fct_order_items: "Факт продаж на уровне позиции заказа",
    cfg.gold_dim_customer: "Измерение: актуальный срез клиентов",
    cfg.gold_dim_product: "Измерение: товары",
    cfg.gold_dim_date: "Измерение: календарь",
}
for table, comment in COMMENTS.items():
    spark.sql(f"COMMENT ON TABLE {table} IS '{comment}'")  # noqa: F821

# COMMAND ----------

# MAGIC %md ## Быстрая проверка результата

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"""
        SELECT order_date, sum(net_revenue) AS revenue, sum(orders_cnt) AS orders
        FROM {cfg.gold_daily_sales}
        GROUP BY order_date ORDER BY order_date
        """
    )
)

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"SELECT segment, count(*) AS customers, round(sum(monetary), 2) AS revenue "
        f"FROM {cfg.gold_customer_rfm} GROUP BY segment ORDER BY revenue DESC"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Дальше
# MAGIC `13_delta_deep_dive.py` — время путешествовать во времени.

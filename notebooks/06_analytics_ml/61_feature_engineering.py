# Databricks notebook source
# MAGIC %md
# MAGIC # Part 6.1 — Feature engineering
# MAGIC
# MAGIC Задача: предсказать отток — уйдёт ли клиент в ближайшие 30 дней.
# MAGIC
# MAGIC Две вещи, которые на реальных проектах ломают ML чаще всего:
# MAGIC
# MAGIC 1. **Утечка целевой переменной.** Фичи считаем строго на дату отсечки
# MAGIC    (`as_of_date`), метку — по окну *после* неё. Если посчитать recency
# MAGIC    по всем данным, модель покажет 0.99 AUC и развалится на проде.
# MAGIC 2. **Несогласованность train/serve.** Поэтому фичи материализуются в
# MAGIC    таблицу `ml.customer_features` с первичным ключом, и обучение с
# MAGIC    инференсом читают одну и ту же таблицу.

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821
dbutils.widgets.text("label_window_days", "30")  # noqa: F821

# COMMAND ----------

from ecom.config import Config
from ecom.utils.logging import get_logger

log = get_logger("features")
cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
label_days = int(dbutils.widgets.get("label_window_days"))  # noqa: F821
spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Дата отсечки
# MAGIC Берём максимум минус окно метки: только так после отсечки остаётся
# MAGIC достаточно данных, чтобы метку вообще можно было посчитать.

# COMMAND ----------

as_of = spark.sql(  # noqa: F821
    f"SELECT date_sub(max(order_date), {label_days}) FROM {cfg.gold_fct_order_items}"
).first()[0]
print(f"as_of_date = {as_of}, окно метки = {label_days} дней")

# COMMAND ----------

# MAGIC %md ## Фичи на дату отсечки

# COMMAND ----------

spark.sql(  # noqa: F821
    f"""
    CREATE OR REPLACE TABLE {cfg.ml_customer_features}
    COMMENT 'Фичи клиентов на дату отсечки для модели оттока'
    AS
    WITH purchases AS (
      SELECT
        customer_id,
        count(DISTINCT order_id)                       AS orders_cnt,
        count(*)                                       AS items_cnt,
        sum(net_amount)                                AS total_spent,
        avg(net_amount)                                AS avg_item_amount,
        max(order_date)                                AS last_order_date,
        min(order_date)                                AS first_order_date,
        count(DISTINCT category)                       AS categories_cnt,
        count(DISTINCT channel)                        AS channels_cnt,
        avg(quantity)                                  AS avg_quantity,
        sum(CASE WHEN NOT is_revenue THEN 1 ELSE 0 END) AS cancelled_items
      FROM {cfg.gold_fct_order_items}
      WHERE order_date <= DATE '{as_of}' AND customer_id IS NOT NULL
      GROUP BY customer_id
    ),
    recent AS (
      SELECT
        customer_id,
        count(DISTINCT order_id) AS orders_last_30d,
        sum(net_amount)          AS spent_last_30d
      FROM {cfg.gold_fct_order_items}
      WHERE order_date BETWEEN date_sub(DATE '{as_of}', 30) AND DATE '{as_of}'
        AND is_revenue AND customer_id IS NOT NULL
      GROUP BY customer_id
    ),
    behaviour AS (
      SELECT
        customer_id,
        count(DISTINCT session_id)                     AS sessions_cnt,
        count(*)                                       AS events_cnt,
        sum(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)     AS add_to_cart_cnt,
        sum(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS remove_from_cart_cnt,
        sum(CASE WHEN is_conversion THEN 1 ELSE 0 END) AS checkout_cnt
      FROM {cfg.silver_clickstream}
      WHERE event_date <= DATE '{as_of}' AND customer_id IS NOT NULL
      GROUP BY customer_id
    )
    SELECT
      c.customer_id,
      DATE '{as_of}'                                          AS as_of_date,
      c.country,
      c.segment,
      datediff(DATE '{as_of}', c.signup_date)                 AS tenure_days,
      coalesce(p.orders_cnt, 0)                               AS orders_cnt,
      coalesce(p.items_cnt, 0)                                AS items_cnt,
      coalesce(p.total_spent, 0)                              AS total_spent,
      coalesce(p.avg_item_amount, 0)                          AS avg_item_amount,
      coalesce(p.categories_cnt, 0)                           AS categories_cnt,
      coalesce(p.channels_cnt, 0)                             AS channels_cnt,
      coalesce(p.avg_quantity, 0)                             AS avg_quantity,
      coalesce(p.cancelled_items, 0)                          AS cancelled_items,
      datediff(DATE '{as_of}', p.last_order_date)             AS recency_days,
      datediff(p.last_order_date, p.first_order_date)         AS active_span_days,
      coalesce(r.orders_last_30d, 0)                          AS orders_last_30d,
      coalesce(r.spent_last_30d, 0)                           AS spent_last_30d,
      coalesce(b.sessions_cnt, 0)                             AS sessions_cnt,
      coalesce(b.events_cnt, 0)                               AS events_cnt,
      coalesce(b.add_to_cart_cnt, 0)                          AS add_to_cart_cnt,
      coalesce(b.remove_from_cart_cnt, 0)                     AS remove_from_cart_cnt,
      coalesce(b.checkout_cnt, 0)                             AS checkout_cnt,
      CASE WHEN coalesce(b.add_to_cart_cnt, 0) = 0 THEN 0.0
           ELSE b.checkout_cnt / b.add_to_cart_cnt END        AS cart_conversion_rate
    FROM {cfg.gold_dim_customer} c
    LEFT JOIN purchases p ON c.customer_id = p.customer_id
    LEFT JOIN recent    r ON c.customer_id = r.customer_id
    LEFT JOIN behaviour b ON c.customer_id = b.customer_id
    WHERE p.customer_id IS NOT NULL   -- только те, кто хоть раз покупал
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Метка
# MAGIC `churned = 1`, если после отсечки клиент не сделал ни одного заказа.
# MAGIC Считается строго по данным ПОСЛЕ `as_of_date` — никакого пересечения с фичами.

# COMMAND ----------

LABELS = cfg.table("ml", "customer_labels")

spark.sql(  # noqa: F821
    f"""
    CREATE OR REPLACE TABLE {LABELS}
    COMMENT 'Метка оттока: не было заказов в течение окна после даты отсечки'
    AS
    SELECT
      f.customer_id,
      f.as_of_date,
      CASE WHEN nxt.orders IS NULL OR nxt.orders = 0 THEN 1 ELSE 0 END AS churned
    FROM {cfg.ml_customer_features} f
    LEFT JOIN (
      SELECT customer_id, count(DISTINCT order_id) AS orders
      FROM {cfg.gold_fct_order_items}
      WHERE order_date > DATE '{as_of}' AND is_revenue
      GROUP BY customer_id
    ) nxt ON f.customer_id = nxt.customer_id
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Первичный ключ
# MAGIC PK на таблице фич — не украшение: он нужен Feature Engineering в UC
# MAGIC для lookup'ов, и он документирует гранулярность таблицы.

# COMMAND ----------

for ddl in [
    f"ALTER TABLE {cfg.ml_customer_features} ALTER COLUMN customer_id SET NOT NULL",
    f"ALTER TABLE {cfg.ml_customer_features} ALTER COLUMN as_of_date SET NOT NULL",
    f"ALTER TABLE {cfg.ml_customer_features} ADD CONSTRAINT pk_customer_features "
    f"PRIMARY KEY (customer_id, as_of_date)",
]:
    try:
        spark.sql(ddl)  # noqa: F821
    except Exception as exc:
        log.warning("skip: %s", type(exc).__name__)

# COMMAND ----------

# MAGIC %md ## Баланс классов и вменяемость фич

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"""
        SELECT churned, count(*) AS customers,
               round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
        FROM {LABELS} GROUP BY churned
        """
    )
)

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"""
        SELECT l.churned,
               round(avg(f.recency_days), 1)   AS avg_recency,
               round(avg(f.orders_cnt), 2)     AS avg_orders,
               round(avg(f.total_spent), 2)    AS avg_spent,
               round(avg(f.sessions_cnt), 1)   AS avg_sessions
        FROM {cfg.ml_customer_features} f
        JOIN {LABELS} l USING (customer_id, as_of_date)
        GROUP BY l.churned
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC Если средние по классам отличаются радикально по **одной** фиче —
# MAGIC это повод заподозрить утечку. Здесь `recency_days` ожидаемо различается:
# MAGIC давно не покупавшие чаще уходят совсем, и это законный сигнал,
# MAGIC а не утечка — он посчитан до даты отсечки.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Дальше
# MAGIC `62_mlflow_train.py`

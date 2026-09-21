-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Part 2.4 — LDP Gold на SQL
-- MAGIC
-- MAGIC В одном пайплайне можно свободно смешивать Python и SQL: движок строит
-- MAGIC общий граф зависимостей по именам таблиц, а не по порядку файлов.
-- MAGIC
-- MAGIC Здесь все витрины — **materialized view**: их нужно пересчитывать целиком
-- MAGIC при изменении источников, и движок сам решит, можно ли сделать это
-- MAGIC инкрементально.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_fct_order_items
COMMENT 'Факт продаж на уровне позиции заказа'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  i.order_item_id,
  i.order_id,
  o.customer_id,
  i.product_id,
  o.order_date,
  o.order_ts,
  o.status,
  o.channel,
  o.shipping_country,
  p.category,
  p.subcategory,
  p.brand,
  i.quantity,
  i.unit_price,
  CAST(i.line_amount AS DECIMAL(14, 2))                          AS gross_amount,
  CAST(i.line_amount * o.discount_pct AS DECIMAL(14, 2))         AS discount_amount,
  CAST(i.line_amount * (1 - o.discount_pct) AS DECIMAL(14, 2))   AS net_amount,
  o.is_revenue
FROM silver_order_items i
JOIN silver_orders  o USING (order_id)
LEFT JOIN silver_products p USING (product_id);

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales
COMMENT 'Продажи по дате, каналу и категории'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  order_date,
  channel,
  category,
  count(DISTINCT order_id)    AS orders_cnt,
  count(DISTINCT customer_id) AS customers_cnt,
  sum(quantity)               AS units,
  sum(net_amount)             AS net_revenue,
  sum(discount_amount)        AS discount_total,
  CAST(sum(net_amount) / count(DISTINCT order_id) AS DECIMAL(14, 2)) AS avg_order_value
FROM gold_fct_order_items
WHERE is_revenue
GROUP BY order_date, channel, category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Витрина с оконной функцией
-- MAGIC Скользящее среднее считается поверх уже агрегированной витрины —
-- MAGIC так граф остаётся плоским, а пересчёт дешёвым.

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_sales_trend
COMMENT 'Дневная выручка со скользящим средним за 7 дней'
AS
SELECT
  order_date,
  channel,
  category,
  net_revenue,
  CAST(avg(net_revenue) OVER (
    PARTITION BY channel, category
    ORDER BY CAST(order_date AS TIMESTAMP)
    RANGE BETWEEN INTERVAL 6 DAYS PRECEDING AND CURRENT ROW
  ) AS DECIMAL(14, 2)) AS net_revenue_7d_avg
FROM gold_daily_sales;

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_customer_360
COMMENT 'Клиент + агрегаты покупок + поведение на сайте'
TBLPROPERTIES ('quality' = 'gold')
AS
WITH purchases AS (
  SELECT
    customer_id,
    max(order_date)          AS last_order_date,
    count(DISTINCT order_id) AS orders_cnt,
    sum(net_amount)          AS lifetime_value
  FROM gold_fct_order_items
  WHERE is_revenue AND customer_id IS NOT NULL
  GROUP BY customer_id
),
behaviour AS (
  SELECT
    customer_id,
    count(DISTINCT session_id)                    AS sessions_cnt,
    sum(CASE WHEN is_conversion THEN 1 ELSE 0 END) AS checkouts_cnt
  FROM silver_clickstream
  WHERE customer_id IS NOT NULL
  GROUP BY customer_id
)
SELECT
  c.customer_id,
  c.full_name,
  c.email,
  c.country,
  c.city,
  c.segment,
  c.signup_date,
  coalesce(p.orders_cnt, 0)     AS orders_cnt,
  coalesce(p.lifetime_value, 0) AS lifetime_value,
  p.last_order_date,
  datediff(current_date(), p.last_order_date) AS days_since_last_order,
  coalesce(b.sessions_cnt, 0)   AS sessions_cnt,
  coalesce(b.checkouts_cnt, 0)  AS checkouts_cnt
FROM silver_customers_current c
LEFT JOIN purchases p ON c.customer_id = p.customer_id
LEFT JOIN behaviour b ON c.customer_id = b.customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Витрина качества данных
-- MAGIC Event log пайплайна доступен как таблица — из него собирается
-- MAGIC собственный отчёт по expectations (подробный разбор в Part 5).

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold_scd2_audit
COMMENT 'Сколько версий у клиентов и когда были изменения'
AS
SELECT
  customer_id,
  count(*)                                        AS versions_cnt,
  min(__START_AT)                                 AS first_seen_at,
  max(__START_AT)                                 AS last_change_at,
  sum(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) AS current_versions
FROM silver_customers_scd2
GROUP BY customer_id
HAVING count(*) > 1;

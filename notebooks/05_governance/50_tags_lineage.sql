-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Part 5.1 — Метаданные, теги и lineage
-- MAGIC
-- MAGIC Unity Catalog хранит не только «где лежит таблица», но и смысл данных.
-- MAGIC Три вещи, которые дают максимум пользы за минимум усилий:
-- MAGIC
-- MAGIC 1. **Комментарии** на таблицах и колонках — их читают люди, Catalog Explorer
-- MAGIC    и Genie (Part 6). Плохие комментарии = плохие ответы natural-language BI.
-- MAGIC 2. **Теги** — машиночитаемая разметка: `pii`, `layer`, `owner`, `sla`.
-- MAGIC    По ним потом строятся политики доступа и отчёты.
-- MAGIC 3. **Lineage** — считается автоматически, но только если вы пишете через
-- MAGIC    Spark/SQL внутри Databricks. Смотреть в Catalog Explorer → Lineage.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'ecom_dev';
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------

-- MAGIC %md ## Комментарии на колонках

-- COMMAND ----------

ALTER TABLE gold.fct_order_items ALTER COLUMN net_amount
  COMMENT 'Выручка по строке заказа после скидки, EUR';
ALTER TABLE gold.fct_order_items ALTER COLUMN is_revenue
  COMMENT 'TRUE для статусов paid/shipped/delivered — только они считаются выручкой';
ALTER TABLE gold.customer_rfm ALTER COLUMN segment
  COMMENT 'RFM-сегмент: champions, loyal, new_or_promising, at_risk_high_value, hibernating';
ALTER TABLE silver.customers_scd2 ALTER COLUMN __END_AT
  COMMENT 'NULL означает актуальную версию записи';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Теги
-- MAGIC Тег — пара ключ-значение. Ставится на каталог, схему, таблицу и колонку.
-- MAGIC Именно колоночные теги дальше используются для масок.

-- COMMAND ----------

ALTER CATALOG IDENTIFIER(:catalog) SET TAGS ('project' = 'ecom-lakehouse', 'env' = 'dev');

ALTER SCHEMA bronze SET TAGS ('layer' = 'bronze', 'retention_days' = '30');
ALTER SCHEMA silver SET TAGS ('layer' = 'silver');
ALTER SCHEMA gold   SET TAGS ('layer' = 'gold', 'consumer' = 'bi');

-- COMMAND ----------

-- Помечаем персональные данные — это разметка, на которую опираются политики
ALTER TABLE silver.customers_scd2 ALTER COLUMN email     SET TAGS ('pii' = 'email');
ALTER TABLE silver.customers_scd2 ALTER COLUMN phone     SET TAGS ('pii' = 'phone');
ALTER TABLE silver.customers_scd2 ALTER COLUMN full_name SET TAGS ('pii' = 'name');
ALTER TABLE gold.dim_customer     ALTER COLUMN email     SET TAGS ('pii' = 'email');
ALTER TABLE gold.dim_customer     ALTER COLUMN full_name SET TAGS ('pii' = 'name');

-- COMMAND ----------

-- MAGIC %md ## Где всё это посмотреть: INFORMATION_SCHEMA

-- COMMAND ----------

SELECT catalog_name, schema_name, table_name, column_name, tag_name, tag_value
FROM information_schema.column_tags
ORDER BY schema_name, table_name, column_name;

-- COMMAND ----------

SELECT table_schema, table_name, comment
FROM information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
ORDER BY table_schema, table_name;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Таблицы без комментариев — техдолг
-- MAGIC Такой запрос полезно повесить алертом: новая таблица без описания —
-- MAGIC это будущий вопрос «а что здесь лежит?».

-- COMMAND ----------

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
  AND (comment IS NULL OR comment = '')
ORDER BY 1, 2;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Lineage
-- MAGIC Программный доступ — через системные таблицы:
-- MAGIC `system.access.table_lineage` и `system.access.column_lineage`.
-- MAGIC Визуально — вкладка **Lineage** в Catalog Explorer на любой таблице.

-- COMMAND ----------

SELECT
  source_table_full_name,
  target_table_full_name,
  entity_type,
  max(event_time) AS last_seen
FROM system.access.table_lineage
WHERE target_table_catalog = :catalog
GROUP BY ALL
ORDER BY last_seen DESC
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Привилегии
-- MAGIC В Free Edition вы один пользователь, поэтому сценарий разграничения
-- MAGIC показывается «на бумаге». На платном воркспейсе это ровно те же команды,
-- MAGIC но с настоящими группами.
-- MAGIC
-- MAGIC ```sql
-- MAGIC GRANT USE CATALOG ON CATALOG ecom_prod TO `analysts`;
-- MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA ecom_prod.gold TO `analysts`;
-- MAGIC REVOKE SELECT ON SCHEMA ecom_prod.bronze FROM `analysts`;
-- MAGIC ALTER SCHEMA ecom_prod.gold OWNER TO `data-engineering`;
-- MAGIC ```
-- MAGIC
-- MAGIC Принцип: аналитикам — только `gold`, инженерам — `bronze`/`silver`,
-- MAGIC владелец — группа, а не человек (иначе увольнение ломает продакшен).

-- COMMAND ----------

SHOW GRANTS ON SCHEMA gold;

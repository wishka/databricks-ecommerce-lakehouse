-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Part 5.2 — Row filters и column masks
-- MAGIC
-- MAGIC Классическая задача: одна таблица, разные права.
-- MAGIC Аналитик по Германии должен видеть только немецкие заказы; email клиента
-- MAGIC видит только служба поддержки, остальные — маску.
-- MAGIC
-- MAGIC Unity Catalog решает это **без копий таблиц**:
-- MAGIC
-- MAGIC * **row filter** — UDF, возвращающая BOOLEAN; применяется к каждой строке;
-- MAGIC * **column mask** — UDF, преобразующая значение колонки при чтении.
-- MAGIC
-- MAGIC Обе функции видят `current_user()` и `is_account_group_member()`,
-- MAGIC поэтому политика пишется один раз для всех.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'ecom_dev';
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Справочник доступа по странам
-- MAGIC Матрицу «кто какие страны видит» держим таблицей, а не хардкодом в UDF:
-- MAGIC менять права тогда можно без правки функции.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ops.access_country (
  principal STRING NOT NULL COMMENT 'email пользователя или имя группы',
  country   STRING NOT NULL COMMENT 'ISO-код страны или * для всех'
)
COMMENT 'Матрица доступа к строкам по странам';

-- COMMAND ----------

-- Выдаём себе полный доступ, чтобы не потерять данные из виду
INSERT INTO ops.access_country
SELECT current_user(), '*'
WHERE NOT EXISTS (SELECT 1 FROM ops.access_country WHERE principal = current_user());

-- Пример ограниченного доступа
MERGE INTO ops.access_country AS t
USING (SELECT 'de-analyst@example.com' AS principal, 'DE' AS country) AS s
ON t.principal = s.principal AND t.country = s.country
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

-- MAGIC %md ## Row filter

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ops.filter_by_country(country STRING)
RETURN
  -- админам и владельцам данных — всё
  is_account_group_member('data-engineering')
  -- остальным — только разрешённые страны
  OR EXISTS (
       SELECT 1 FROM ops.access_country a
       WHERE a.principal = current_user()
         AND (a.country = '*' OR a.country = country)
     );

-- COMMAND ----------

ALTER TABLE gold.fct_order_items
  SET ROW FILTER ops.filter_by_country ON (shipping_country);

-- COMMAND ----------

-- Проверка: под своим пользователем видно всё (у нас '*')
SELECT shipping_country, count(*) AS rows
FROM gold.fct_order_items
GROUP BY shipping_country
ORDER BY rows DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Column mask
-- MAGIC Маска не прячет колонку, а меняет значение. Это важно: запросы,
-- MAGIC которые ссылаются на колонку, не ломаются.

-- COMMAND ----------

CREATE OR REPLACE FUNCTION ops.mask_email(email STRING)
RETURN CASE
  WHEN is_account_group_member('customer-support') OR is_account_group_member('data-engineering')
    THEN email
  WHEN email IS NULL THEN NULL
  -- оставляем домен: агрегаты по доменам всё ещё считаются
  ELSE concat('***@', split_part(email, '@', 2))
END;

CREATE OR REPLACE FUNCTION ops.mask_name(value STRING)
RETURN CASE
  WHEN is_account_group_member('customer-support') OR is_account_group_member('data-engineering')
    THEN value
  ELSE concat(left(value, 1), '***')
END;

-- COMMAND ----------

ALTER TABLE gold.dim_customer ALTER COLUMN email     SET MASK ops.mask_email;
ALTER TABLE gold.dim_customer ALTER COLUMN full_name SET MASK ops.mask_name;

-- COMMAND ----------

SELECT customer_id, full_name, email, country, segment
FROM gold.dim_customer
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Где посмотреть, что политики применены

-- COMMAND ----------

SELECT table_schema, table_name, column_name, mask_name
FROM information_schema.column_masks
WHERE table_catalog = :catalog;

-- COMMAND ----------

SELECT table_schema, table_name, filter_name
FROM information_schema.row_filters
WHERE table_catalog = :catalog;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Снять политики
-- MAGIC Пригодится, если захотите перепроверить «сырые» цифры.
-- MAGIC
-- MAGIC ```sql
-- MAGIC ALTER TABLE gold.fct_order_items DROP ROW FILTER;
-- MAGIC ALTER TABLE gold.dim_customer ALTER COLUMN email DROP MASK;
-- MAGIC ALTER TABLE gold.dim_customer ALTER COLUMN full_name DROP MASK;
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Подводные камни, о которых стоит знать
-- MAGIC
-- MAGIC * Функция политики выполняется **на каждую строку** — держите её простой,
-- MAGIC   тяжёлый подзапрос внутри маски убивает производительность.
-- MAGIC * Row filter применяется и к джобам: если джоба бежит под пользователем
-- MAGIC   с ограничением, витрина посчитается по урезанным данным. Поэтому ETL
-- MAGIC   запускают от service principal с полным доступом, а фильтры вешают
-- MAGIC   на витрины, а не на исходные таблицы.
-- MAGIC * Маска не защищает от вывода через агрегаты — это контроль доступа,
-- MAGIC   а не анонимизация.

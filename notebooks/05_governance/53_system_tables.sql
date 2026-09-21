-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Part 5.4 — System tables: аудит, стоимость, история запросов
-- MAGIC
-- MAGIC `system` — каталог, который Databricks ведёт сам. Три схемы, ради которых
-- MAGIC стоит туда ходить:
-- MAGIC
-- MAGIC | Схема | Что внутри | Зачем DE |
-- MAGIC |---|---|---|
-- MAGIC | `system.access` | аудит действий, lineage таблиц и колонок | «кто удалил таблицу», влияние изменений |
-- MAGIC | `system.billing` | потребление DBU по ресурсам | FinOps: какая джоба съедает бюджет |
-- MAGIC | `system.query` | история SQL-запросов и их профили | поиск тяжёлых запросов |
-- MAGIC | `system.lakeflow` | метаданные джоб и их запусков | SLA, доля падений, длительность |
-- MAGIC
-- MAGIC Схемы включаются администратором и наполняются с задержкой (обычно
-- MAGIC до нескольких часов). В Free Edition доступен не весь набор — часть
-- MAGIC запросов ниже может вернуть ошибку «table not found», это ожидаемо.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'ecom_dev';

-- COMMAND ----------

SHOW SCHEMAS IN system;

-- COMMAND ----------

-- MAGIC %md ## Аудит: что происходило с нашими данными

-- COMMAND ----------

SELECT
  event_time,
  user_identity.email AS actor,
  service_name,
  action_name,
  request_params.full_name_arg AS object_name
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 7 DAYS
  AND action_name IN ('createTable', 'deleteTable', 'commandSubmit', 'getTable')
  AND request_params.full_name_arg LIKE concat(:catalog, '%')
ORDER BY event_time DESC
LIMIT 100;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Кто читает наши витрины
-- MAGIC Полезно перед тем, как что-то удалять или переименовывать.

-- COMMAND ----------

SELECT
  source_table_full_name AS table_name,
  created_by,
  count(*)        AS reads,
  max(event_time) AS last_read
FROM system.access.table_lineage
WHERE source_table_catalog = :catalog
  AND event_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY reads DESC
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Стоимость
-- MAGIC `system.billing.usage` в связке с `list_prices` даёт деньги, а не абстрактные DBU.

-- COMMAND ----------

SELECT
  usage_date,
  sku_name,
  round(sum(usage_quantity), 2) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY usage_date DESC, dbus DESC;

-- COMMAND ----------

-- Сколько стоит каждая джоба
SELECT
  u.usage_metadata.job_id      AS job_id,
  round(sum(u.usage_quantity * p.pricing.default), 2) AS estimated_usd,
  round(sum(u.usage_quantity), 2) AS dbus
FROM system.billing.usage u
JOIN system.billing.list_prices p
  ON u.sku_name = p.sku_name
 AND u.usage_end_time >= p.price_start_time
 AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS
  AND u.usage_metadata.job_id IS NOT NULL
GROUP BY ALL
ORDER BY estimated_usd DESC
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md ## Тяжёлые запросы

-- COMMAND ----------

SELECT
  statement_id,
  executed_by,
  round(total_duration_ms / 1000.0, 1) AS seconds,
  round(read_bytes / 1024 / 1024 / 1024.0, 2) AS read_gb,
  read_rows,
  left(statement_text, 120) AS statement_preview
FROM system.query.history
WHERE start_time >= current_timestamp() - INTERVAL 7 DAYS
  AND statement_type = 'SELECT'
ORDER BY total_duration_ms DESC
LIMIT 25;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## SLA джоб
-- MAGIC Доля падений и длительность запусков — то, что должно висеть на дашборде
-- MAGIC дата-команды.

-- COMMAND ----------

SELECT
  j.name AS job_name,
  count(*) AS runs,
  sum(CASE WHEN r.result_state = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
  round(100.0 * sum(CASE WHEN r.result_state <> 'SUCCEEDED' THEN 1 ELSE 0 END) / count(*), 1)
    AS failure_pct,
  round(avg(unix_timestamp(r.period_end_time) - unix_timestamp(r.period_start_time)) / 60.0, 1)
    AS avg_minutes
FROM system.lakeflow.job_run_timeline r
JOIN system.lakeflow.jobs j ON r.job_id = j.job_id
WHERE r.period_start_time >= current_timestamp() - INTERVAL 30 DAYS
GROUP BY ALL
ORDER BY failure_pct DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Идея для алерта
-- MAGIC Повесьте Databricks Alert на запрос выше с условием `failure_pct > 10`
-- MAGIC — и вы узнаете о деградации раньше, чем это заметят аналитики.

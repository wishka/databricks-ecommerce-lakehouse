-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Part 0 — Unity Catalog: каталог, схемы, volume
-- MAGIC
-- MAGIC Запускать на serverless SQL warehouse или в ноутбуке.
-- MAGIC
-- MAGIC Что здесь важно понять:
-- MAGIC * трёхуровневое имя `catalog.schema.object` — в UC нет «просто таблицы»;
-- MAGIC * **managed volume** — это файловое хранилище внутри UC, замена DBFS;
-- MAGIC   путь `/Volumes/<catalog>/<schema>/<volume>/...` виден и питону, и Spark;
-- MAGIC * **managed table** — данными и файлами управляет Databricks;
-- MAGIC   в Free Edition external location недоступен, поэтому всё managed.

-- COMMAND ----------

-- MAGIC %md ## Параметр окружения
-- MAGIC Виджет `catalog` позволяет одним и тем же кодом создать `ecom_dev` и `ecom_prod`.

-- COMMAND ----------

CREATE WIDGET TEXT catalog DEFAULT 'ecom_dev';

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS IDENTIFIER(:catalog)
  COMMENT 'E-commerce lakehouse pet-project';

-- COMMAND ----------

USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS raw    COMMENT 'Исходные файлы (volume) и служебные объекты приёма';
CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Сырые данные как есть + технические поля';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Типизированные, дедуплицированные, провалидированные данные';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Витрины и звезда для BI/ML';
CREATE SCHEMA IF NOT EXISTS ops    COMMENT 'Служебное: качество данных, аудит запусков, карантин';
CREATE SCHEMA IF NOT EXISTS ml     COMMENT 'Фичи и модели';
CREATE SCHEMA IF NOT EXISTS ldp    COMMENT 'Выход Lakeflow Declarative Pipeline (Part 2)';

-- COMMAND ----------

-- MAGIC %md ## Volume для входящих файлов

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS raw.landing
  COMMENT 'Landing zone: JSON/CSV, которые забирает Auto Loader';

-- COMMAND ----------

-- MAGIC %md ## Проверка

-- COMMAND ----------

SHOW SCHEMAS;

-- COMMAND ----------

DESCRIBE VOLUME raw.landing;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Служебные таблицы ops
-- MAGIC Аудит запусков — единственная таблица, которую заводим заранее:
-- MAGIC в неё пишут все джобы, и её схема не должна зависеть от порядка запуска.

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS ops.pipeline_audit (
  run_id        STRING    NOT NULL,
  job_name      STRING    NOT NULL,
  step_name     STRING    NOT NULL,
  target_table  STRING,
  started_at    TIMESTAMP NOT NULL,
  finished_at   TIMESTAMP,
  status        STRING,
  rows_written  BIGINT,
  error_message STRING
)
USING DELTA
COMMENT 'Единый журнал запусков всех шагов пайплайна'
CLUSTER BY (job_name, started_at);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Дальше
-- MAGIC `01_generate_raw_data.py` — сгенерировать исходные файлы в volume.

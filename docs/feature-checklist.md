# Чек-лист функционала Databricks

Отмечайте по мере прохождения — это и трекер, и шпаргалка перед собеседованием.

## Платформа и Unity Catalog

- [ ] Workspace, serverless notebooks, SQL warehouse (Part 0)
- [ ] Catalog → schema → table, трёхуровневые имена (Part 0)
- [ ] Managed volumes вместо DBFS (Part 0)
- [ ] Git folders + GitHub (Part 0 / 4)
- [ ] Комментарии на таблицах и колонках (Part 1 / 5)
- [ ] Теги на каталоге, схемах, колонках (Part 5)
- [ ] `INFORMATION_SCHEMA` (Part 5)
- [ ] GRANT / REVOKE / ownership (Part 5)
- [ ] Row filters (Part 5)
- [ ] Column masks (Part 5)
- [ ] Lineage таблиц и колонок (Part 5)
- [ ] System tables: access, billing, query, lakeflow (Part 5)

## Приём данных

- [ ] Auto Loader: `cloudFiles`, schema location, inferColumnTypes (Part 1)
- [ ] Schema evolution `addNewColumns` (Part 1)
- [ ] `rescuedDataColumn` (Part 1)
- [ ] `_metadata` (file_path, modification_time) (Part 1)
- [ ] `trigger(availableNow=True)` — батч поверх стриминга (Part 1)
- [ ] `maxFilesPerTrigger` (Part 1 / 3)
- [ ] CSV и JSON в одном пайплайне (Part 1)

## Delta Lake

- [ ] ACID и транзакционный лог, `DESCRIBE HISTORY` / `DESCRIBE DETAIL` (Part 1.4)
- [ ] Time travel `VERSION AS OF`, `RESTORE` (Part 1.4)
- [ ] `MERGE INTO`: upsert (Part 1.2)
- [ ] `WHEN NOT MATCHED BY SOURCE` — soft delete (Part 1.4)
- [ ] SCD Type 2 через MERGE (Part 1.2)
- [ ] Change Data Feed (Part 1.4)
- [ ] `OPTIMIZE` (Part 1.3)
- [ ] Liquid clustering и его отличие от партиционирования (Part 1.4)
- [ ] `VACUUM` и его риски (Part 1.4)
- [ ] `SHALLOW CLONE` (Part 1.4)
- [ ] `mergeSchema` / `overwriteSchema` / column mapping (Part 1.4)
- [ ] Delta constraints: `NOT NULL`, `CHECK`, informational PK/FK (Part 1.3)

## Spark

- [ ] Оконные функции: `row_number`, `ntile`, `dense_rank`, rolling `RANGE` (Part 1.3)
- [ ] Broadcast join и чтение плана `EXPLAIN` (Part 1.3)
- [ ] AQE (Part 1.4)
- [ ] Детерминированный дедуп (Part 1.2)
- [ ] DECIMAL вместо float в деньгах (Part 1.2)

## Lakeflow Spark Declarative Pipelines

- [ ] `@dp.table` — streaming table (Part 2.1)
- [ ] `@dp.materialized_view` (Part 2.2)
- [ ] `@dp.temporary_view` (Part 2.3)
- [ ] `@dp.expect_all` / `_or_drop` / `_or_fail` (Part 2.2)
- [ ] `dp.create_streaming_table` (Part 2.3)
- [ ] `dp.create_auto_cdc_flow` — SCD 1 и 2 (Part 2.3)
- [ ] `@dp.append_flow` — несколько источников в одну таблицу (Part 2)
- [ ] Python и SQL в одном пайплайне (Part 2.4)
- [ ] Event log как таблица UC, разбор метрик качества (Part 2.5)
- [ ] Triggered vs continuous, development vs production (Part 2)

## Structured Streaming

- [ ] `readStream` / `writeStream`, чекпоинты (Part 3)
- [ ] Event time vs processing time (Part 3)
- [ ] Watermark и `numRowsDroppedByWatermark` (Part 3)
- [ ] Tumbling window (Part 3)
- [ ] `dropDuplicatesWithinWatermark` (Part 3)
- [ ] `foreachBatch` + `MERGE` (Part 3)
- [ ] Stream-static join (Part 3)
- [ ] Output modes: append / update / complete (Part 3)
- [ ] `StreamingQueryListener` (Part 3)

## Оркестрация и CI/CD

- [ ] Lakeflow Jobs: multi-task граф, `depends_on` (Part 4)
- [ ] Job parameters и `base_parameters` (Part 4)
- [ ] Retries, timeouts, `run_if` (Part 4)
- [ ] Schedule (cron) и continuous jobs (Part 4)
- [ ] Health rules и email-уведомления (Part 4)
- [ ] Databricks Asset Bundles: targets, variables, artifacts (Part 4)
- [ ] `bundle validate / deploy / run` (Part 4)
- [ ] Wheel как зависимость джоб и пайплайна (Part 4)
- [ ] pytest поверх локального PySpark (Part 4)
- [ ] GitHub Actions: lint, тесты, деплой (Part 4)
- [ ] Секреты: GitHub Secrets / Databricks secrets (Part 4)

## Качество данных

- [ ] Декларативный реестр правил, общий для batch и LDP (Part 1 / 2 / 5)
- [ ] Карантин-таблицы с перечнем нарушенных правил (Part 1.2)
- [ ] Метрики качества по запускам и тренд (Part 5.3)
- [ ] Блокирующие правила, роняющие джобу (Part 5.3)

## Аналитика и ML

- [ ] SQL warehouse и параметризованные запросы (Part 6)
- [ ] AI/BI Dashboard с фильтрами и расписанием (Part 6)
- [ ] Genie space поверх Gold (Part 6)
- [ ] Databricks Alerts (Part 6)
- [ ] MLflow: эксперимент, autolog, сигнатура модели (Part 6.2)
- [ ] Реестр моделей в Unity Catalog, алиасы champion/challenger (Part 6.2)
- [ ] `mlflow.pyfunc.spark_udf` — распределённый батч-инференс (Part 6.3)
- [ ] Предотвращение утечки целевой переменной (Part 6.1)
- [ ] Простейший мониторинг дрейфа (Part 6.3)

## Недоступно в Free Edition

- [ ] ~~Классические кластеры, cluster policies, init scripts~~
- [ ] ~~Scala / R~~
- [ ] ~~External locations, storage credentials~~
- [ ] ~~Delta Sharing, Clean Rooms~~
- [ ] ~~Model serving на GPU, provisioned throughput~~
- [ ] ~~Несколько workspace, SCIM, SSO~~

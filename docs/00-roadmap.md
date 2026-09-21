# Roadmap: от нуля до полного покрытия Databricks

Проект разбит на **7 частей** (Part 0 — подготовка + 6 содержательных).
Каждая часть самодостаточна: её можно сделать за 1–2 вечера, она даёт рабочий артефакт
и закрывает свой блок функционала платформы.

Домен: **e-commerce** (заказы, позиции заказов, клиенты, товары, клик-стрим).
Данные — синтетические, генерируются внутри Databricks (Free Edition ограничивает
исходящий интернет, поэтому качать датасеты извне ненадёжно).

---

## Карта частей

| # | Часть | Что осваиваем | Артефакт |
|---|-------|---------------|----------|
| 0 | Fundamentals & Setup | Workspace, Unity Catalog, Volumes, Git folders, serverless | `ecom_dev` каталог + сгенерированные raw-файлы |
| 1 | Batch ingestion & Medallion | Auto Loader, Delta Lake, MERGE, SCD, OPTIMIZE, CDF, time travel | Bronze → Silver → Gold на PySpark |
| 2 | Declarative Pipelines (LDP) | Streaming tables, materialized views, expectations, AUTO CDC | Пайплайн `ecommerce_ldp` |
| 3 | Structured Streaming | Watermark, stateful aggregations, foreachBatch, checkpoints | Near-real-time витрина сессий |
| 4 | Orchestration & CI/CD | Lakeflow Jobs, Asset Bundles, pytest, GitHub Actions | `databricks.yml` + зелёный CI |
| 5 | Governance & Data Quality | Теги, lineage, row filters, column masks, system tables, DQ-фреймворк | `ops.dq_results` + политики доступа |
| 6 | Analytics & ML | SQL warehouse, AI/BI Dashboard, Genie, MLflow, batch inference | Дашборд + модель churn в UC |

---

## Part 0 — Fundamentals & Setup

**Цель:** рабочее окружение и данные, на которых всё остальное строится.

Осваиваем:
- Workspace: Notebooks, Editor, Catalog Explorer, Compute (serverless).
- Unity Catalog: catalog → schema → table/volume, трёхуровневые имена.
- Managed Volumes — файловое хранилище внутри UC (замена DBFS).
- Git folders: подключение GitHub-репозитория прямо в workspace.
- Serverless notebooks и SQL warehouse 2X-Small.

Делаем:
1. `notebooks/00_setup/00_create_catalog.sql` — каталоги `ecom_dev` / `ecom_prod`,
   схемы `raw / bronze / silver / gold / ops / ml`, volume `raw.landing`.
2. `notebooks/00_setup/01_generate_raw_data.py` — генератор синтетики
   (`src/ecom/datagen/generator.py`): пишет JSON/CSV/Parquet в volume,
   в том числе «грязные» записи и late-arriving events — они понадобятся в Part 1 и 5.

Критерий готовности: в Catalog Explorer видно каталог `ecom_dev`,
в volume `raw/landing` лежат батчи файлов за несколько дат.

---

## Part 1 — Batch ingestion & Medallion (PySpark + Delta)

**Цель:** классический ETL-слой, который и спрашивают на собеседованиях.

Осваиваем:
- **Auto Loader** (`cloudFiles`): инкрементальный приём файлов, schema inference
  и schema evolution, `rescuedDataColumn`, `_metadata`.
- **Delta Lake**: ACID, `MERGE INTO`, upsert, дедупликация, `DESCRIBE HISTORY`,
  time travel (`VERSION AS OF`), `OPTIMIZE` + liquid clustering, `VACUUM`,
  Change Data Feed, `CREATE TABLE ... CLONE`.
- **SCD Type 2** руками через MERGE (в Part 2 сравним с декларативным AUTO CDC).
- Витрины Gold: звезда (`dim_*` / `fct_*`), окна, RFM-сегментация.
- Оптимизация: broadcast join, AQE, партиционирование vs liquid clustering,
  чтение плана через `EXPLAIN`.

Делаем: `notebooks/01_batch/10..13`, логика — в `src/ecom/transforms/`.

Критерий готовности: `gold.daily_sales` и `gold.customer_rfm` пересчитываются
идемпотентно — повторный запуск не меняет результат.

---

## Part 2 — Lakeflow Spark Declarative Pipelines

**Цель:** тот же медальон, но декларативно — и увидеть разницу.

Осваиваем:
- `from pyspark import pipelines as dp`: `@dp.table`, `@dp.materialized_view`,
  `@dp.temporary_view`, `dp.create_streaming_table`.
- Expectations: `@dp.expect`, `@dp.expect_or_drop`, `@dp.expect_all_or_fail` —
  и их метрики в event log.
- **AUTO CDC** (`dp.create_auto_cdc_flow`) — SCD1 и SCD2 из коробки.
- `@dp.append_flow` для объединения нескольких источников в одну таблицу.
- Event log пайплайна как таблица: разбор качества и производительности.
- Triggered vs continuous режим, development vs production.

Делаем: `pipelines/ecommerce_ldp/*` + `resources/pipelines/ecommerce_ldp.yml`.

> Free Edition: **один активный пайплайн каждого типа**. Поэтому LDP-пайплайн
> здесь ровно один, и он пишет в отдельную схему `ldp`, не конфликтуя с Part 1.

Критерий готовности: в UI пайплайна виден граф bronze → silver → gold,
на silver отображается процент отброшенных записей по expectations.

---

## Part 3 — Structured Streaming

**Цель:** near-real-time слой и понимание состояния/времени события.

Осваиваем:
- `readStream` / `writeStream`, `availableNow` vs `processingTime`.
- Checkpoints, exactly-once, идемпотентная запись.
- Event time, **watermark**, tumbling/sliding/session windows.
- Stream-static join (обогащение справочником) и dedup по ключу.
- `foreachBatch` + `MERGE` — upsert из стрима.
- Мониторинг: `StreamingQueryListener`, метрики прогресса.

Делаем: `notebooks/03_streaming/30_clickstream_producer.py` (пишет события в volume
порциями) и `31_streaming_aggregations.py` (читает Auto Loader'ом, считает
5-минутные окна, апсертит в `gold.sessions_5min`).

Критерий готовности: остановили стрим, дописали файлы, запустили снова —
данные досчитались без дублей.

---

## Part 4 — Orchestration & CI/CD

**Цель:** проект как продукт, а не набор ноутбуков.

Осваиваем:
- **Lakeflow Jobs**: multi-task граф, зависимости, `run_if`, retries,
  параметры и `job parameters`, task values, notifications, schedule / file arrival trigger.
- **Databricks Asset Bundles** (`databricks.yml`): targets `dev` / `prod`,
  переменные, `bundle validate / deploy / run`.
- Юнит-тесты трансформаций на локальном PySpark (`pytest`, `chispa`-style сравнения).
- GitHub Actions: lint (`ruff`) + тесты на PR, деплой бандла на push в `main`.
- Секреты: `databricks secrets` / GitHub Secrets, никакого PAT в коде.

Делаем: `databricks.yml`, `resources/jobs/*.yml`, `tests/*`, `.github/workflows/*`.

Критерий готовности: `databricks bundle deploy -t dev` создаёт джобы и пайплайн,
`bundle run` прогоняет весь медальон end-to-end.

---

## Part 5 — Governance & Data Quality

**Цель:** то, что отличает pet-проект от «ещё одного ETL».

Осваиваем:
- UC-привилегии: `GRANT`, группы, ownership, `INFORMATION_SCHEMA`.
- Комментарии и **теги** на таблицах/колонках (PII-маркировка).
- **Row filters** и **column masks** — построчный и поколоночный доступ.
- Lineage в Catalog Explorer (таблицы и колонки).
- **System tables**: `system.access.audit`, `system.billing.usage`,
  `system.query.history` — аудит и стоимость.
- Собственный DQ-фреймворк: декларативные правила → `ops.dq_results` →
  квaрантин-таблица + алерт.
- Delta constraints (`CHECK`), `NOT NULL`, primary/foreign key (informational).

Делаем: `notebooks/05_governance/*`, `src/ecom/quality/*`.

Критерий готовности: запуск `ops` -джобы наполняет `ops.dq_results`,
битые заказы уезжают в `ops.quarantine_orders`, PII-колонка замаскирована.

---

## Part 6 — Analytics & ML

**Цель:** данные превращаются в ответы.

Осваиваем:
- SQL warehouse, `sql/queries/*.sql`, кэш, `EXPLAIN` в SQL editor.
- **AI/BI Dashboard**: датасеты, параметры, фильтры, расписание.
- **Genie space** поверх Gold — natural language BI, инструкции и примеры вопросов.
- **Alerts** на SQL-запрос (падение выручки / рост доли отменённых заказов).
- **MLflow**: эксперименты, автологирование, регистрация модели в Unity Catalog,
  алиасы `@champion`, батч-инференс в Gold-таблицу.
- Feature engineering в UC (таблицы фич с PK).

Делаем: `notebooks/06_analytics_ml/*`, `sql/queries/*`, `sql/dashboards/*`.

Критерий готовности: дашборд обновляется по расписанию, модель
`ecom_dev.ml.churn_model@champion` зарегистрирована, `gold.customer_churn_scores` заполнена.

---

## Что остаётся за скобками (и почему)

| Фича | Статус в Free Edition |
|------|----------------------|
| Классические (non-serverless) кластеры, cluster policies, init scripts | Недоступны — только serverless |
| Scala / R | Не поддерживаются |
| External locations / storage credentials, свой S3 | Недоступны — только managed storage |
| Delta Sharing, Clean Rooms | Недоступны |
| Model serving на GPU, provisioned throughput | Недоступны |
| Account console, SCIM, SSO, несколько workspace | Один workspace, один metastore |

Если позже появится Premium/Trial-воркспейс — Part 1 и Part 4 расширяются
внешним location (S3/ADLS), кластерными политиками и Delta Sharing без переписывания логики.

---

## Порядок работы с Git

1. Репозиторий на GitHub, ветка `main` защищена.
2. В Databricks: **Workspace → Repos/Git folders → Add → GitHub URL**.
   Токен GitHub заводится в **Settings → Linked accounts**.
3. Разработка — в ветке `feature/partN-...`, PR → CI (`ruff` + `pytest`) → merge.
4. `main` деплоится бандлом в target `dev` (и вручную в `prod`).

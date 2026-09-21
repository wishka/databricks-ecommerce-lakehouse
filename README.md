# E-commerce Lakehouse on Databricks

Pet-проект Data Engineer, который последовательно закрывает **весь основной функционал
Databricks**: Unity Catalog, Auto Loader, Delta Lake, Lakeflow Spark Declarative Pipelines,
Structured Streaming, Lakeflow Jobs, Asset Bundles, governance, AI/BI и MLflow.

Работает на **Databricks Free Edition** (serverless-only), но без изменений
разворачивается и на платном воркспейсе.

```
raw files (Volume)
      │  Auto Loader
      ▼
   BRONZE  ──► SILVER ──► GOLD ──► Dashboard / Genie / ML
      │           │          │
      │           │          └── gold.daily_sales, customer_rfm, sessions_5min
      │           └── SCD2 customers, cleaned orders, DQ quarantine
      └── raw + _metadata + rescued data
```

## Структура

```
docs/                  роадмап, инструкции по частям, архитектура
notebooks/             ноутбуки по частям (00 setup → 06 ML)
pipelines/             исходники Lakeflow Declarative Pipeline
src/ecom/              переиспользуемая логика (тестируемая локально)
resources/             ресурсы Asset Bundle: jobs, pipelines
sql/                   SQL-витрины, запросы и дашборд
tests/                 pytest поверх локального PySpark
.github/workflows/     CI (lint + tests) и деплой бандла
databricks.yml         Databricks Asset Bundle
```

## Быстрый старт

### 1. Databricks

```sql
-- notebooks/00_setup/00_create_catalog.sql
```
Запустить в SQL editor или ноутбуке на serverless. Создаст каталог `ecom_dev`,
схемы и volume `ecom_dev.raw.landing`.

Затем `notebooks/00_setup/01_generate_raw_data.py` — сгенерирует исходные файлы.

### 2. Локально (тесты и бандл)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest -q                     # юнит-тесты трансформаций на локальном Spark
ruff check .                  # линт
```

### 3. Asset Bundle

```bash
# Free Edition: авторизация по PAT
export DATABRICKS_HOST=https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...

databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run ecom_batch_medallion -t dev
```

## Части проекта

Подробный план — в [`docs/00-roadmap.md`](docs/00-roadmap.md).

| Часть | Тема | Документ |
|-------|------|----------|
| 0 | Setup, Unity Catalog, генерация данных | [docs/part-00-setup.md](docs/part-00-setup.md) |
| 1 | Auto Loader, Delta, медальон на PySpark | [docs/part-01-batch.md](docs/part-01-batch.md) |
| 2 | Lakeflow Declarative Pipelines | [docs/part-02-ldp.md](docs/part-02-ldp.md) |
| 3 | Structured Streaming | [docs/part-03-streaming.md](docs/part-03-streaming.md) |
| 4 | Jobs, Asset Bundles, CI/CD | [docs/part-04-cicd.md](docs/part-04-cicd.md) |
| 5 | Governance и Data Quality | [docs/part-05-governance.md](docs/part-05-governance.md) |
| 6 | Analytics и ML | [docs/part-06-analytics-ml.md](docs/part-06-analytics-ml.md) |

Чек-лист «что из Databricks уже потрогано» — [docs/feature-checklist.md](docs/feature-checklist.md).

## Модель данных

**Bronze** — сырьё как есть + `_metadata`, `_rescued_data`, `_ingested_at`.

**Silver** — типизировано, дедуплицировано, провалидировано:
`silver.orders`, `silver.order_items`, `silver.products`,
`silver.customers_scd2` (+ view `silver.customers_current`), `silver.clickstream_events`.

**Gold** — звезда и витрины:
`gold.dim_customer`, `gold.dim_product`, `gold.dim_date`, `gold.fct_order_items`,
`gold.daily_sales`, `gold.customer_rfm`, `gold.sessions_5min`, `gold.customer_churn_scores`.

**Ops** — служебное: `ops.dq_results`, `ops.quarantine_orders`, `ops.pipeline_audit`.

## Требования

- Databricks Free Edition (или любой воркспейс с Unity Catalog)
- Python 3.11+ локально, `databricks-cli` ≥ 0.240
- Аккаунт GitHub

## Лицензия

MIT — см. [LICENSE](LICENSE).

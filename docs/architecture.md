# Архитектура

## Потоки данных

```
                        ┌──────────────────────────────┐
                        │  /Volumes/<cat>/raw/landing  │
                        │  orders · order_items ·      │
                        │  customers · products ·      │
                        │  clickstream                 │
                        └───────────────┬──────────────┘
                                        │ Auto Loader
                    ┌───────────────────┴────────────────────┐
                    │                                        │
          ┌─────────▼──────────┐                  ┌──────────▼──────────┐
          │  Part 1: PySpark   │                  │  Part 2: LDP        │
          │  bronze.* →        │                  │  ldp.bronze_* →     │
          │  silver.* →        │                  │  ldp.silver_* →     │
          │  gold.*            │                  │  ldp.gold_*         │
          │  (MERGE, SCD2)     │                  │  (AUTO CDC, expect) │
          └─────────┬──────────┘                  └──────────┬──────────┘
                    │                                        │
                    │            ┌───────────────────────────┘
                    │            │  ops.ldp_event_log
                    ▼            ▼
          ┌───────────────────────────────┐
          │ ops.dq_results                │  ← Part 5
          │ ops.quarantine_*              │
          │ ops.pipeline_audit            │
          └───────────────────────────────┘

  landing/clickstream_live ──► Part 3 Structured Streaming ──► gold.sessions_5min

  gold.* ──► ml.customer_features ──► MLflow ──► gold.customer_churn_scores  (Part 6)
         └─► AI/BI Dashboard, Genie space, Alerts
```

## Почему два медальона

Part 1 (PySpark) и Part 2 (Lakeflow Declarative Pipelines) решают одну задачу
двумя способами и пишут в разные схемы (`bronze/silver/gold` против `ldp`).
Это сделано намеренно:

* видно, во что разворачивается декларативный подход;
* можно сравнить производительность, объём кода и удобство отладки;
* обе ветки живут одновременно, не мешая друг другу.

Общее у них — реестр DQ-правил `ecom.quality.rules`: batch-ветка исполняет его
через `evaluate_rules`, LDP — через `@dp.expect_all*`. Одна декларация, две реализации.

## Слои

| Слой | Правило | Что запрещено |
|---|---|---|
| `bronze` | данные как есть + `_source_file`, `_ingested_at`, `_rescued_data` | менять значения, фильтровать |
| `silver` | типы, дедуп, нормализация, DQ | бизнес-агрегаты, join'ы ради витрин |
| `gold` | звезда и витрины, готовые к BI | сырые/неочищенные данные |
| `ops` | качество, аудит, карантин | бизнес-данные |
| `ml` | фичи и модели | всё остальное |

## Идемпотентность

Каждый шаг можно перезапустить без последствий:

* **bronze** — Auto Loader помнит обработанные файлы в чекпоинте;
* **silver** — `overwrite` из bronze; дедуп детерминирован (`row_number` по `_ingested_at`);
* **SCD2** — MERGE сравнивает хэш отслеживаемых колонок: повтор того же снимка
  не создаёт новую версию;
* **gold** — полный пересчёт `overwrite` из silver;
* **streaming** — `foreachBatch` + `MERGE` по ключу окна.

## Именование

```
<catalog>.<schema>.<table>

ecom_dev.bronze.orders_raw
ecom_dev.silver.customers_scd2
ecom_dev.gold.fct_order_items
ecom_dev.ops.dq_results
ecom_dev.ml.customer_features
```

Суррогатные ключи в Gold — детерминированный `sha2(natural_key)`, а не
`monotonically_increasing_id()`: последний меняется при каждом пересчёте
и ломает ссылки между фактом и измерением.

## Конфигурация

Ни одного захардкоженного каталога в коде. Значение приходит:

```
job parameter → notebook widget → ecom.config.Config(catalog=...)
bundle variable ${var.catalog} → pipeline configuration → spark.conf ecom.catalog
```

Смена окружения — это смена значения переменной бандла, а не правка кода.

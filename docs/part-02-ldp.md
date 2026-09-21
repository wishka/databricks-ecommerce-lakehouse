# Part 2 — Lakeflow Spark Declarative Pipelines

**Время:** 2–3 часа. **Результат:** пайплайн `ecommerce_ldp`, считающий тот же
медальон декларативно, с метриками качества в event log.

## Файлы

```
pipelines/ecommerce_ldp/
├── 00_bronze.py          streaming tables поверх Auto Loader
├── 01_silver.py          expectations из общего реестра
├── 02_customers_scd2.py  AUTO CDC: SCD 1 и SCD 2
└── 03_gold.sql           materialized views на SQL
resources/pipelines/ecommerce_ldp.yml   конфигурация пайплайна
```

## Как запустить

**Через бандл (рекомендуется):**

```bash
databricks bundle deploy -t dev
databricks bundle run ecommerce_ldp -t dev
```

**Через UI:** Jobs & Pipelines → Create pipeline → Serverless →
source: Git folder, путь `pipelines/ecommerce_ldp` → catalog `ecom_dev`, schema `ldp`.

## Разница с Part 1 — в цифрах

| | Part 1 (PySpark) | Part 2 (LDP) |
|---|---|---|
| Строк кода на медальон | ~450 | ~250 |
| Чекпоинты | ведём сами | движок |
| Порядок выполнения | руками в джобе | граф строится сам |
| Метрики качества | пишем в `ops.dq_results` | в event log из коробки |
| SCD2 | MERGE с UNION-трюком | `create_auto_cdc_flow(...)` |
| Отладка отдельного шага | запустил ячейку | только через пайплайн |
| Контроль над записью | полный | ограниченный |

Вывод, который стоит уметь формулировать: LDP выигрывает там, где пайплайн
типовой (приём → очистка → витрины) и важна прозрачность качества.
Ручной PySpark — там, где нужна нестандартная запись, сложная оркестрация
или интеграция с внешними системами внутри шага.

## Expectations

```python
@dp.expect_all_or_fail(ORDERS_FAIL)   # нарушено → падает весь update
@dp.expect_all_or_drop(ORDERS_DROP)   # строка отбрасывается
@dp.expect_all(ORDERS_WARN)           # только метрика
```

Правила берутся из `ecom.quality.rules` — того же реестра, что использует
batch-ветка. Работает это благодаря строке в конфиге пайплайна:

```yaml
environment:
  dependencies:
    - --editable ${workspace.file_path}
```

Она ставит пакет проекта из задеплоенных файлов воркспейса, и пайплайн может
импортировать `ecom`. Приём стоит запомнить: он избавляет от копипасты
бизнес-правил между пайплайном и остальным кодом.

## AUTO CDC

```python
dp.create_streaming_table(name="silver_customers_scd2")

dp.create_auto_cdc_flow(
    target="silver_customers_scd2",
    source="customers_cdc",
    keys=["customer_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type="2",
    except_column_list=["updated_at"],
)
```

Движок сам: упорядочивает события по `sequence_by`, дедуплицирует по ключу,
закрывает предыдущие версии, ставит `__START_AT` / `__END_AT` и остаётся
идемпотентным при перезапуске. Сравните с `src/ecom/transforms/scd.py`.

Соседний параметр `track_history_except_column_list` оставляет колонку
в таблице, но её изменение не создаёт новую версию — так поступают
с техническими полями, чтобы история не распухала.

## Event log

В конфиге пайплайна event log опубликован как таблица UC:

```yaml
event_log:
  catalog: ${var.catalog}
  schema: ops
  name: ldp_event_log
```

Разбор — в `notebooks/02_ldp/20_pipeline_event_log.py`: процент отброшенных
строк по каждому правилу, длительность и объём каждого flow, ошибки.
Это самый наглядный артефакт всего проекта.

## Ограничение Free Edition

Активен может быть **один пайплайн каждого типа**. Поэтому пайплайн здесь один,
и он пишет в схему `ldp`, не пересекаясь с Part 1. Если захотите второй —
сначала остановите и удалите первый.

## Частые проблемы

**`ModuleNotFoundError: ecom`** — пайплайн развёрнут не бандлом либо
`environment.dependencies` не применился. Проверьте Settings → Environment
у пайплайна; при запуске из UI зависимость нужно добавить руками.

**Таблица «пропала» после смены имени функции** — имя целевой таблицы берётся
из `name=` или из имени функции. Переименовали функцию без `name=` — получили
новую таблицу, старая осталась сиротой.

**`development: true` держит ресурсы** — это не баг: в dev-режиме движок не
пересоздаёт окружение между запусками, чтобы стартовать быстрее.

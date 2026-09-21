# Part 1 — Batch: Auto Loader, Delta, медальон

**Время:** 3–5 часов. **Результат:** `gold.daily_sales` и `gold.customer_rfm`,
пересчитываемые идемпотентно.

## Ноутбуки

| Ноутбук | О чём |
|---|---|
| `01_batch/10_bronze_autoloader.py` | приём файлов, schema evolution, `_rescued_data` |
| `01_batch/11_silver_transform.py` | типизация, дедуп, DQ, карантин, SCD2 |
| `01_batch/12_gold_marts.py` | звезда, витрины, constraints |
| `01_batch/13_delta_deep_dive.py` | лаборатория по Delta Lake |

Запускать по порядку.

## Ключевые идеи

### Auto Loader ≠ `spark.read`

`spark.read.json(path)` читает всё каждый раз. Auto Loader ведёт реестр
обработанных файлов в чекпоинте, поэтому второй запуск обработает только новое.
`trigger(availableNow=True)` превращает стрим в «догони и остановись» —
это правильный режим для расписания и он дешевле постоянно работающего стрима.

### Дедупликация должна быть детерминированной

```python
# плохо: при повторе может остаться другая строка
df.dropDuplicates(["order_id"])

# хорошо: побеждает последняя по времени приёма
w = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())
df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
```

### Silver ничего не выбрасывает молча

Нарушившие правила строки уходят в `ops.quarantine_<dataset>` с колонкой
`_dq_failed_rules`. Если данные просто отфильтровать, вы никогда не узнаете,
что источник сломался — упадёт только выручка на дашборде, и неделю спустя.

### SCD Type 2 одним MERGE

Приём с UNION: для каждой изменившейся строки в staging кладутся две записи —
одна с `merge_key = customer_id` (матчится → закрывает текущую версию) и одна
с `merge_key = NULL` (никогда не матчится → вставляется как новая версия).
Сравнение идёт по `sha2` от отслеживаемых колонок, поэтому повторная загрузка
того же снимка не плодит версии.

Инвариант, который проверяется в ноутбуке: **ровно одна** строка
с `is_current = true` на каждого клиента.

### DECIMAL, а не DOUBLE

Деньги считаются в `DECIMAL(14,2)`. `DOUBLE` даёт `0.1 + 0.2 = 0.30000000000000004`,
и расхождение в копейку между витриной и бухгалтерией — ваша проблема.

### Суррогатные ключи

`sha2(natural_key)`, а не `monotonically_increasing_id()`: последний меняется
при каждом пересчёте и рвёт связь между фактом и измерением.

## Delta deep dive: что попробовать руками

1. Запустите silver дважды и сравните `DESCRIBE HISTORY` — сколько версий, какие операции.
2. Сделайте `RESTORE TABLE ... TO VERSION AS OF` и убедитесь, что откат — тоже версия.
3. Посмотрите CDF после MERGE: `update_preimage` и `update_postimage` идут парой.
4. Сравните `EXPLAIN` запроса с фильтром по кластеризованной колонке и без —
   разница в числе прочитанных файлов.
5. Сделайте `SHALLOW CLONE` и проверьте, что он занимает ~0 байт.

## Проверка результата

```sql
-- идемпотентность: второй запуск не меняет цифры
SELECT count(*), round(sum(net_revenue), 2) FROM ecom_dev.gold.daily_sales;

-- инвариант SCD2
SELECT customer_id, count(*) FROM ecom_dev.silver.customers_scd2
WHERE is_current GROUP BY 1 HAVING count(*) <> 1;   -- должно быть пусто

-- карантин
SELECT rule, count(*) FROM ecom_dev.ops.quarantine_orders
LATERAL VIEW explode(_dq_failed_rules) t AS rule GROUP BY rule;
```

## Частые проблемы

**MERGE падает с `multiple source rows matched`** — в источнике несколько строк
на один ключ. Перед MERGE нужен `dedupe_latest_sql`.

**Auto Loader не видит новые файлы** — проверьте, что пишете в тот же путь,
и что `schemaLocation` не удалён. Полный сброс: виджет `full_refresh = true`.

**`_rescued_data` внезапно непустой** — в источнике появилось новое поле или
тип перестал приводиться. Это не баг Auto Loader, это сигнал про источник.

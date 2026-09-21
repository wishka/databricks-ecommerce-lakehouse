# Part 5 — Governance и Data Quality

**Время:** 2–3 часа. **Результат:** размеченные PII, работающие маски и
row filter, отчёт по качеству с трендом.

## Ноутбуки

| Ноутбук | О чём |
|---|---|
| `05_governance/50_tags_lineage.sql` | комментарии, теги, INFORMATION_SCHEMA, lineage |
| `05_governance/51_row_column_security.sql` | row filters, column masks |
| `05_governance/52_dq_report.py` | проверки уровня витрин, тренд, карантин |
| `05_governance/53_system_tables.sql` | аудит, стоимость, история запросов, SLA джоб |

## Теги и комментарии

Комментарий на таблице — не формальность: его читают Catalog Explorer, поиск
и Genie. Плохое описание → плохие ответы natural-language BI (Part 6).

Теги — машиночитаемая разметка (`pii=email`, `layer=gold`, `retention_days=30`).
На них опираются политики доступа и отчёты «где у нас персональные данные».

Полезный запрос — таблицы без описания, готовый кандидат в алерт:

```sql
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_schema IN ('bronze','silver','gold') AND (comment IS NULL OR comment = '');
```

## Row filter и column mask

Одна таблица, разные права — **без копий**.

```sql
CREATE OR REPLACE FUNCTION ops.filter_by_country(country STRING)
RETURN is_account_group_member('data-engineering')
    OR EXISTS (SELECT 1 FROM ops.access_country a
               WHERE a.principal = current_user() AND (a.country = '*' OR a.country = country));

ALTER TABLE gold.fct_order_items SET ROW FILTER ops.filter_by_country ON (shipping_country);
```

```sql
CREATE OR REPLACE FUNCTION ops.mask_email(email STRING)
RETURN CASE WHEN is_account_group_member('customer-support') THEN email
            ELSE concat('***@', split_part(email, '@', 2)) END;

ALTER TABLE gold.dim_customer ALTER COLUMN email SET MASK ops.mask_email;
```

Матрица доступа вынесена в таблицу `ops.access_country`, а не захардкожена
в функцию: права меняются без правки кода.

Три вещи, о которых стоит помнить:

* функция выполняется **на каждую строку** — тяжёлый подзапрос внутри маски
  убивает производительность;
* фильтр применяется и к джобам: ETL должен бежать от принципала с полным
  доступом, иначе витрина посчитается по урезанным данным. Поэтому политики
  вешают на витрины, а не на исходные таблицы;
* маска — это контроль доступа, а не анонимизация: через агрегаты значение
  всё ещё может утечь.

В Free Edition вы единственный пользователь, поэтому эффект видно только
на своей роли — но команды ровно те же, что на продакшене.

## DQ-фреймворк

Правило = SQL-выражение, которое должно быть TRUE:

```python
Rule("orders_customer_known", "orders", "customer_id IS NOT NULL", "drop",
     "Заказ без клиента не попадает в клиентские витрины")
```

Три уровня: `fail` (роняем джобу), `drop` (в карантин), `warn` (только метрика).
Один реестр используют и batch (`evaluate_rules`), и LDP (`@dp.expect_all*`).

Что даёт такая конструкция:

* `ops.dq_results` — история метрик по запускам;
* `ops.quarantine_<dataset>` — сами битые строки с перечнем нарушенных правил;
* сравнение с предыдущим запуском: **дельта важнее абсолютного значения**.
  2% брака — норма, если вчера было 2%; и авария, если вчера было 0.1%.

## System tables

| Таблица | Вопрос, на который отвечает |
|---|---|
| `system.access.audit` | кто и когда удалил/изменил объект |
| `system.access.table_lineage` | кто читает эту витрину (перед тем как её менять) |
| `system.billing.usage` + `list_prices` | какая джоба съедает бюджет |
| `system.query.history` | какие запросы самые тяжёлые |
| `system.lakeflow.job_run_timeline` | доля падений и длительность джоб |

Наполняются с задержкой (до нескольких часов), в Free Edition доступен
не весь набор — часть запросов может вернуть «table not found», это нормально.

## Проверка

```sql
SELECT * FROM information_schema.column_masks WHERE table_catalog = 'ecom_dev';
SELECT * FROM information_schema.row_filters  WHERE table_catalog = 'ecom_dev';

SELECT dataset, rule_name, severity, failure_rate
FROM ecom_dev.ops.dq_results
QUALIFY row_number() OVER (PARTITION BY dataset, rule_name ORDER BY run_ts DESC) = 1
ORDER BY failure_rate DESC;
```

## Частые проблемы

**После включения маски пропали данные** — функция маски вернула NULL,
потому что пользователь не в группе, а ветка `ELSE` не предусмотрена.

**Row filter сломал витрину** — джоба бежит под пользователем с ограничением.
Снимите фильтр с исходной таблицы и поставьте на витрину.

**`ALTER TABLE ... SET TAGS` не проходит** — нужны права `APPLY TAG` на объект.

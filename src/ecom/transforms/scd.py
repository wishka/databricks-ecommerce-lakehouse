"""SCD Type 2 «руками» через Delta MERGE.

В Part 2 то же самое делается одной строкой `dp.create_auto_cdc_flow(...)`.
Здесь — развёрнутый вариант, который полезно понимать: именно его спрашивают
на собеседованиях и именно он нужен, когда декларативного движка нет.

Классический двухшаговый MERGE:
  1) закрыть текущую версию строки (`__END_AT`, `is_current = false`)
     для ключей, у которых пришло изменение;
  2) вставить новые версии.

Чтобы уложиться в один MERGE, используется приём с UNION. Staging состоит
из двух веток:

* **A** — по строке на каждую входящую запись, `merge_key = customer_id`.
  Совпала с текущей версией и хэш отличается → закрываем её.
  Не совпала → это новый клиент, вставляем первую версию.
* **B** — `merge_key = NULL` (никогда не совпадёт → всегда INSERT),
  но **только** для клиентов, у которых отслеживаемые колонки реально
  изменились. Именно это условие даёт идемпотентность: повторная загрузка
  того же снимка не создаёт новых версий.

Частая ошибка в этом паттерне — брать в ветку B все входящие строки.
Тогда каждый повторный запуск плодит дубликаты версий.
"""

from __future__ import annotations

TRACKED_COLUMNS_DEFAULT = ("full_name", "email", "phone", "country", "city", "segment")


def create_scd2_table_sql(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
  customer_id     STRING   NOT NULL,
  full_name       STRING,
  email           STRING,
  phone           STRING,
  country         STRING,
  city            STRING,
  segment         STRING,
  signup_date     DATE,
  is_email_valid  BOOLEAN,
  __START_AT      TIMESTAMP NOT NULL,
  __END_AT        TIMESTAMP,
  is_current      BOOLEAN  NOT NULL,
  _hash           STRING   NOT NULL
)
USING DELTA
CLUSTER BY (customer_id, is_current)
COMMENT 'SCD Type 2 по клиентам: история изменений атрибутов'
TBLPROPERTIES (delta.enableChangeDataFeed = true)
"""


def scd2_merge_sql(
    target: str,
    source: str,
    key: str = "customer_id",
    sequence_by: str = "updated_at",
    tracked_columns: tuple[str, ...] = TRACKED_COLUMNS_DEFAULT,
) -> str:
    """SQL одного прохода SCD2.

    `source` — временное представление с новыми/изменёнными строками
    (по одной актуальной строке на ключ).
    """
    hash_expr = (
        "sha2(concat_ws('||', "
        + ", ".join(f"coalesce(s.{c}, '~')" for c in tracked_columns)
        + "), 256)"
    )
    insert_cols = [key, *tracked_columns, "signup_date", "is_email_valid"]
    insert_cols = list(dict.fromkeys(insert_cols))  # ключ мог попасть в tracked
    select_cols = ", ".join(f"s.{c}" for c in insert_cols)
    insert_list = ", ".join(insert_cols)
    values_list = ", ".join(f"staged.{c}" for c in insert_cols)

    return f"""
MERGE INTO {target} AS t
USING (
  -- A: каждая входящая строка.
  --    matched + изменился хэш → закрываем текущую версию
  --    not matched             → новый клиент, вставляем первую версию
  SELECT
    s.{key} AS merge_key, {select_cols},
    s.{sequence_by} AS effective_from, {hash_expr} AS row_hash
  FROM {source} s

  UNION ALL

  -- B: merge_key = NULL → всегда INSERT, но только для реально изменившихся.
  --    Без условия `cur._hash <> ...` повторный запуск плодил бы дубли версий.
  SELECT
    NULL AS merge_key, {select_cols},
    s.{sequence_by} AS effective_from, {hash_expr} AS row_hash
  FROM {source} s
  JOIN {target} cur
    ON cur.{key} = s.{key} AND cur.is_current = true
  WHERE cur._hash <> {hash_expr}
    AND s.{sequence_by} > cur.__START_AT
) AS staged
ON t.{key} = staged.merge_key AND t.is_current = true

WHEN MATCHED AND t._hash <> staged.row_hash AND staged.effective_from > t.__START_AT
THEN UPDATE SET
  t.__END_AT   = staged.effective_from,
  t.is_current = false

WHEN NOT MATCHED THEN INSERT ({insert_list}, __START_AT, __END_AT, is_current, _hash)
VALUES ({values_list}, staged.effective_from, NULL, true, staged.row_hash)
"""


def current_view_sql(view: str, scd_table: str) -> str:
    return f"""
CREATE OR REPLACE VIEW {view}
COMMENT 'Актуальный срез клиентов из SCD2-таблицы'
AS SELECT
  customer_id, full_name, email, phone, country, city, segment,
  signup_date, is_email_valid, __START_AT AS valid_from
FROM {scd_table}
WHERE is_current = true
"""


def dedupe_latest_sql(source_table: str, key: str = "customer_id", sequence_by: str = "updated_at") -> str:
    """Оставить по одной (последней) строке на ключ — вход для MERGE."""
    return f"""
SELECT * EXCEPT (_rn) FROM (
  SELECT *, row_number() OVER (PARTITION BY {key} ORDER BY {sequence_by} DESC) AS _rn
  FROM {source_table}
) WHERE _rn = 1
"""

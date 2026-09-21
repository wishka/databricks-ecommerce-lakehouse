"""Тесты SCD2-логики.

Проверяем не только сгенерированный SQL, но и его исполнение на локальном Delta:
именно на MERGE чаще всего и ломается SCD2 (несколько совпадений на ключ,
незакрытая предыдущая версия, «мигающие» дубли текущих строк).
"""

from __future__ import annotations

import pytest

from ecom.transforms.scd import (
    create_scd2_table_sql,
    current_view_sql,
    dedupe_latest_sql,
    scd2_merge_sql,
)


def test_merge_sql_contains_union_trick():
    sql = scd2_merge_sql("t", "s")
    assert sql.count("FROM s s") == 2, "две ветки: закрывающая и вставляющая"
    assert "NULL AS merge_key" in sql
    assert "WHEN MATCHED AND t._hash <> staged.row_hash" in sql
    assert "cur.is_current = true" in sql


def test_merge_sql_insert_branch_is_guarded():
    """Ветка B обязана фильтроваться по изменению хэша — иначе повторный
    запуск создаст дубликат версии для каждого клиента."""
    sql = scd2_merge_sql("t", "s")
    branch_b = sql.split("UNION ALL", 1)[1]

    assert "NULL AS merge_key" in branch_b
    assert "JOIN t cur" in branch_b
    assert "WHERE cur._hash <>" in branch_b


def test_merge_sql_respects_custom_key():
    sql = scd2_merge_sql("t", "s", key="account_id", sequence_by="event_time")
    assert "t.account_id = staged.merge_key" in sql
    assert "s.event_time AS effective_from" in sql


@pytest.fixture
def scd_env(spark):
    from ecom.utils.spark import delta_enabled

    if not delta_enabled(spark):
        pytest.skip("Delta недоступна локально — интеграционный тест SCD2 пропущен")
    spark.sql("CREATE DATABASE IF NOT EXISTS scd_test")
    spark.sql("DROP TABLE IF EXISTS scd_test.customers")
    spark.sql(create_scd2_table_sql("scd_test.customers").replace("CLUSTER BY (customer_id, is_current)", ""))
    yield "scd_test.customers"
    spark.sql("DROP TABLE IF EXISTS scd_test.customers")


def _load(spark, rows):
    df = spark.createDataFrame(
        rows,
        "customer_id string, full_name string, email string, phone string, country string, "
        "city string, segment string, signup_date date, is_email_valid boolean, updated_at timestamp",
    )
    df.createOrReplaceTempView("updates")


def test_scd2_first_load_creates_current_version(spark, scd_env):
    import datetime as dt

    _load(
        spark,
        [
            ("C1", "Anna", "a@e.com", "1", "DE", "Berlin", "consumer", dt.date(2025, 1, 1), True, dt.datetime(2026, 8, 1)),
        ],
    )
    spark.sql(scd2_merge_sql(scd_env, "updates"))

    rows = spark.table(scd_env).collect()
    assert len(rows) == 1
    assert rows[0]["is_current"] is True
    assert rows[0]["__END_AT"] is None


def test_scd2_change_closes_previous_version(spark, scd_env):
    import datetime as dt

    _load(spark, [("C1", "Anna", "a@e.com", "1", "DE", "Berlin", "consumer", dt.date(2025, 1, 1), True, dt.datetime(2026, 8, 1))])
    spark.sql(scd2_merge_sql(scd_env, "updates"))

    _load(spark, [("C1", "Anna", "a@e.com", "1", "DE", "Munich", "vip", dt.date(2025, 1, 1), True, dt.datetime(2026, 8, 5))])
    spark.sql(scd2_merge_sql(scd_env, "updates"))

    rows = sorted(spark.table(scd_env).collect(), key=lambda r: r["__START_AT"])
    assert len(rows) == 2

    old, new = rows
    assert old["is_current"] is False
    assert old["__END_AT"] == new["__START_AT"], "старая версия закрывается началом новой"
    assert new["is_current"] is True
    assert new["city"] == "Munich" and new["segment"] == "vip"


def test_scd2_is_idempotent(spark, scd_env):
    """Повторный MERGE тем же снимком не должен создавать новую версию."""
    import datetime as dt

    _load(spark, [("C1", "Anna", "a@e.com", "1", "DE", "Berlin", "consumer", dt.date(2025, 1, 1), True, dt.datetime(2026, 8, 1))])
    spark.sql(scd2_merge_sql(scd_env, "updates"))
    before = spark.table(scd_env).count()

    spark.sql(scd2_merge_sql(scd_env, "updates"))
    after = spark.table(scd_env).count()

    assert before == after == 1


def test_scd2_exactly_one_current_per_key(spark, scd_env):
    import datetime as dt

    for day, city in [(1, "Berlin"), (5, "Munich"), (9, "Hamburg")]:
        _load(
            spark,
            [("C1", "Anna", "a@e.com", "1", "DE", city, "consumer", dt.date(2025, 1, 1), True, dt.datetime(2026, 8, day))],
        )
        spark.sql(scd2_merge_sql(scd_env, "updates"))

    current = spark.table(scd_env).where("is_current").collect()
    assert len(current) == 1
    assert current[0]["city"] == "Hamburg"


def test_dedupe_latest_sql_shape():
    sql = dedupe_latest_sql("src", key="customer_id", sequence_by="updated_at")
    assert "row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC)" in sql
    assert "_rn = 1" in sql


def test_current_view_sql_filters_current():
    sql = current_view_sql("v", "t")
    assert "WHERE is_current = true" in sql

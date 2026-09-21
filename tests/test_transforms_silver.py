"""Тесты Silver-трансформаций.

Что именно проверяем — и почему это не формальность:

* дедуп **детерминирован** (побеждает последняя по `_ingested_at` запись);
* неизвестный статус не исчезает, а превращается в `unknown`;
* пустая строка в `customer_id` становится NULL (иначе join даст ложное совпадение);
* суммы считаются в DECIMAL, а не в float.
"""

from __future__ import annotations

from decimal import Decimal

from ecom.transforms.silver import (
    clean_customers,
    clean_order_items,
    clean_orders,
)


def test_orders_dedup_keeps_latest_ingested(orders_raw):
    result = clean_orders(orders_raw)
    rows = {r["order_id"]: r for r in result.collect()}

    assert result.count() == 4, "O1 пришёл дважды — должна остаться одна строка"
    assert rows["O1"]["status"] == "shipped", "побеждает запись с большим _ingested_at"


def test_orders_normalizes_values(orders_raw):
    rows = {r["order_id"]: r for r in clean_orders(orders_raw).collect()}

    assert rows["O1"]["channel"] == "web"
    assert rows["O1"]["currency"] == "EUR"
    assert rows["O1"]["shipping_country"] == "DE"
    assert rows["O1"]["is_revenue"] is True
    assert rows["O3"]["status"] == "unknown", "неизвестный статус не теряется"
    assert rows["O3"]["is_revenue"] is False


def test_orders_keeps_broken_rows_for_quarantine(orders_raw):
    """Silver ничего не выбрасывает молча — фильтрация происходит в DQ-слое."""
    rows = {r["order_id"]: r for r in clean_orders(orders_raw).collect()}

    assert rows["O2"]["customer_id"] is None
    assert rows["O4"]["order_ts"] is None
    assert rows["O4"]["order_date"] is None


def test_order_items_amount_and_dedup(order_items_raw):
    result = clean_order_items(order_items_raw)
    rows = {r["order_item_id"]: r for r in result.collect()}

    assert result.count() == 4, "OI4 пришёл дважды"
    assert rows["OI1"]["line_amount"] == Decimal("20.00")
    assert rows["OI2"]["quantity"] == -1, "отрицательное количество доезжает до DQ"
    assert rows["OI3"]["unit_price"] == Decimal("-5.00")


def test_customers_email_validation_and_case(customers_raw):
    rows = clean_customers(customers_raw).collect()
    by_key = {(r["customer_id"], str(r["updated_at"])): r for r in rows}

    anna_v1 = next(r for k, r in by_key.items() if k[0] == "C1" and "08-01" in k[1])
    assert anna_v1["email"] == "anna.berg@example.com"
    assert anna_v1["city"] == "Berlin"
    assert anna_v1["is_email_valid"] is True

    piotr = next(r for r in rows if r["customer_id"] == "C2")
    assert piotr["is_email_valid"] is False, "битый email помечается, но строка остаётся"


def test_customers_keeps_all_versions(customers_raw):
    """Для SCD2 нам нужны ВСЕ версии — дедуп только по (id, updated_at)."""
    result = clean_customers(customers_raw)
    c1_versions = result.where("customer_id = 'C1'").count()
    assert c1_versions == 2

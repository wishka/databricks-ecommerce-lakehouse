"""Тесты Gold-витрин: считаем на маленьком наборе, где ответ известен руками."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal

import pytest

from ecom.transforms.gold import (
    build_customer_rfm,
    build_daily_sales,
    build_dim_date,
    build_fct_order_items,
)


@pytest.fixture
def silver_set(spark):
    orders = spark.createDataFrame(
        [
            ("O1", "C1", dt.datetime(2026, 8, 1, 10), dt.date(2026, 8, 1), "paid", "web", "EUR", "DE", 0.1, True),
            ("O2", "C2", dt.datetime(2026, 8, 1, 12), dt.date(2026, 8, 1), "delivered", "ios", "EUR", "FR", 0.0, True),
            ("O3", "C1", dt.datetime(2026, 8, 2, 9), dt.date(2026, 8, 2), "cancelled", "web", "EUR", "DE", 0.0, False),
        ],
        "order_id string, customer_id string, order_ts timestamp, order_date date, status string, "
        "channel string, currency string, shipping_country string, discount_pct double, is_revenue boolean",
    )
    items = spark.createDataFrame(
        [
            ("OI1", "O1", "P1", 2, Decimal("50.00"), Decimal("100.00")),
            ("OI2", "O2", "P2", 1, Decimal("30.00"), Decimal("30.00")),
            ("OI3", "O3", "P1", 5, Decimal("50.00"), Decimal("250.00")),
        ],
        "order_item_id string, order_id string, product_id string, quantity int, "
        "unit_price decimal(12,2), line_amount decimal(14,2)",
    )
    products = spark.createDataFrame(
        [
            ("P1", "Phone", "Electronics", "Phones", "Aurora", Decimal("55.00"), True),
            ("P2", "Lamp", "Home", "Lighting", "Vela", Decimal("35.00"), True),
        ],
        "product_id string, product_name string, category string, subcategory string, "
        "brand string, list_price decimal(12,2), is_active boolean",
    )
    return orders, items, products


def test_fct_applies_discount(silver_set):
    orders, items, products = silver_set
    rows = {r["order_item_id"]: r for r in build_fct_order_items(orders, items, products).collect()}

    # 100.00 с 10% скидкой → 90.00
    assert rows["OI1"]["gross_amount"] == Decimal("100.00")
    assert rows["OI1"]["discount_amount"] == Decimal("10.00")
    assert rows["OI1"]["net_amount"] == Decimal("90.00")
    assert rows["OI1"]["category"] == "Electronics"

    assert rows["OI2"]["net_amount"] == Decimal("30.00"), "без скидки net == gross"


def test_fct_keeps_non_revenue_rows(silver_set):
    """Отменённый заказ остаётся в факте — но с флагом is_revenue = false.
    Иначе невозможно посчитать долю отмен."""
    orders, items, products = silver_set
    fct = build_fct_order_items(orders, items, products)

    assert fct.count() == 3
    assert fct.where("NOT is_revenue").count() == 1


def test_daily_sales_excludes_non_revenue(silver_set):
    orders, items, products = silver_set
    fct = build_fct_order_items(orders, items, products)
    daily = build_daily_sales(fct).collect()

    assert all(r["order_date"] == dt.date(2026, 8, 1) for r in daily), "2 августа только отмена"
    total = sum(r["net_revenue"] for r in daily)
    assert total == Decimal("120.00")


def test_daily_sales_avg_order_value(silver_set):
    orders, items, products = silver_set
    fct = build_fct_order_items(orders, items, products)
    rows = {(r["channel"], r["category"]): r for r in build_daily_sales(fct).collect()}

    web = rows[("web", "Electronics")]
    assert web["orders_cnt"] == 1
    assert web["avg_order_value"] == Decimal("90.00")


def test_rfm_segments_assigned(silver_set):
    orders, items, products = silver_set
    fct = build_fct_order_items(orders, items, products)
    rfm = build_customer_rfm(fct, as_of=dt.date(2026, 8, 10)).collect()

    assert {r["customer_id"] for r in rfm} == {"C1", "C2"}
    for row in rfm:
        assert row["recency_days"] == 9
        assert 1 <= row["r_score"] <= 5
        assert row["segment"] is not None


def test_dim_date_covers_range(spark):
    dim = build_dim_date(spark, dt.date(2026, 8, 1), dt.date(2026, 8, 7)).collect()

    assert len(dim) == 7
    weekend = [r for r in dim if r["is_weekend"]]
    assert {r["date_key"] for r in weekend} == {dt.date(2026, 8, 1), dt.date(2026, 8, 2)}

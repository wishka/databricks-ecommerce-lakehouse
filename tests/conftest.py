"""Локальный Spark для юнит-тестов.

Тесты проверяют модули из `src/ecom`, а не ноутбуки: ноутбук — это оркестрация,
логика живёт в пакете, поэтому CI не нужен ни Databricks, ни сеть.
"""

from __future__ import annotations

import pytest

from ecom.utils.spark import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("ecom-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def orders_raw(spark):
    """Bronze-подобный вход: всё строками, с дублем и мусором."""
    rows = [
        # order_id, customer_id, order_ts, status, channel, currency, country, discount, ingested
        ("O1", "C1", "2026-08-01T10:00:00", "PAID", "Web", "eur", "de", "0.1", "2026-08-02T00:00:00"),
        ("O1", "C1", "2026-08-01T10:00:00", "shipped", "web", "EUR", "DE", "0.1", "2026-08-03T00:00:00"),
        ("O2", None, "2026-08-01T11:00:00", "paid", "ios", "EUR", "FR", "0.0", "2026-08-02T00:00:00"),
        ("O3", "C2", "2026-08-01T12:00:00", "WEIRD", "web", "EUR", "NL", "0.0", "2026-08-02T00:00:00"),
        ("O4", "C3", None, "delivered", "android", "EUR", "PL", "0.2", "2026-08-02T00:00:00"),
    ]
    return spark.createDataFrame(
        rows,
        "order_id string, customer_id string, order_ts string, status string, channel string, "
        "currency string, shipping_country string, discount_pct string, _ingested_at string",
    )


@pytest.fixture
def order_items_raw(spark):
    rows = [
        ("OI1", "O1", "P1", "2", "10.00", "2026-08-02T00:00:00"),
        ("OI2", "O1", "P2", "-1", "20.00", "2026-08-02T00:00:00"),
        ("OI3", "O2", "P1", "1", "-5.00", "2026-08-02T00:00:00"),
        ("OI4", "O3", "P3", "3", "7.50", "2026-08-02T00:00:00"),
        ("OI4", "O3", "P3", "3", "7.50", "2026-08-03T00:00:00"),  # дубль
    ]
    return spark.createDataFrame(
        rows,
        "order_item_id string, order_id string, product_id string, quantity string, "
        "unit_price string, _ingested_at string",
    )


@pytest.fixture
def customers_raw(spark):
    rows = [
        ("C1", "Anna Berg", "Anna.Berg@Example.com", "+491", "de", "berlin", "Consumer", "2025-01-01", "2026-08-01T00:00:00"),
        ("C1", "Anna Berg", "Anna.Berg@Example.com", "+491", "de", "munich", "vip", "2025-01-01", "2026-08-05T00:00:00"),
        ("C2", "Piotr Nowak", "piotr#example.com", "+482", "pl", "warsaw", "pro", "2025-06-01", "2026-08-01T00:00:00"),
    ]
    return spark.createDataFrame(
        rows,
        "customer_id string, full_name string, email string, phone string, country string, "
        "city string, segment string, signup_date string, updated_at string",
    )

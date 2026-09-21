"""Тесты генератора: он должен быть воспроизводимым и «управляемо грязным».

Если генератор недетерминирован, то и тесты, и демо перестают быть
повторяемыми — поэтому seed проверяется явно.
"""

from __future__ import annotations

import datetime as dt

from ecom.datagen.generator import (
    evolve_customers,
    generate_clickstream,
    generate_customers,
    generate_day,
    generate_products,
)

DAY = dt.date(2026, 8, 1)


def test_products_are_reproducible():
    a = generate_products(n=50, seed=1)
    b = generate_products(n=50, seed=1)
    c = generate_products(n=50, seed=2)

    assert a == b
    assert a != c


def test_customers_have_unique_ids():
    customers = generate_customers(n=200, seed=5)
    assert len({c["customer_id"] for c in customers}) == 200


def test_generate_day_produces_items_for_every_order():
    customers = generate_customers(n=50, seed=1)
    products = generate_products(n=20, seed=1)
    orders, items = generate_day(DAY, customers, products, orders_per_day=100, seed=0, dup_ratio=0.0)

    order_ids = {o["order_id"] for o in orders}
    item_order_ids = {i["order_id"] for i in items}
    assert item_order_ids <= order_ids
    assert len(item_order_ids) == len(order_ids), "у каждого заказа есть хотя бы одна позиция"


def test_generate_day_injects_duplicates():
    customers = generate_customers(n=50, seed=1)
    products = generate_products(n=20, seed=1)
    orders, _ = generate_day(DAY, customers, products, orders_per_day=200, seed=0, dup_ratio=0.05)

    assert len(orders) > len({o["order_id"] for o in orders}), "дубли должны быть"


def test_generate_day_injects_dirty_rows():
    customers = generate_customers(n=50, seed=1)
    products = generate_products(n=20, seed=1)
    orders, items = generate_day(
        DAY, customers, products, orders_per_day=300, seed=0, dirty_ratio=0.1
    )

    assert any(o["customer_id"] is None for o in orders)
    assert any(i["quantity"] <= 0 for i in items)


def test_evolve_customers_returns_only_changed():
    customers = generate_customers(n=300, seed=5)
    changed = evolve_customers(customers, as_of=dt.date(2026, 8, 2), change_ratio=0.1)

    assert 0 < len(changed) < len(customers)
    ids = {c["customer_id"] for c in customers}
    assert {c["customer_id"] for c in changed} <= ids

    by_id = {c["customer_id"]: c for c in customers}
    for row in changed:
        old = by_id[row["customer_id"]]
        assert row["updated_at"] > old["updated_at"]


def test_clickstream_sessions_are_coherent():
    customers = generate_customers(n=30, seed=1)
    products = generate_products(n=10, seed=1)
    events = generate_clickstream(DAY, customers, products, n_events=400, seed=1)

    assert len({e["event_id"] for e in events}) == len(events)

    by_session: dict[str, set] = {}
    for event in events:
        by_session.setdefault(event["session_id"], set()).add(event["device"])
    assert all(len(devices) == 1 for devices in by_session.values()), "устройство внутри сессии одно"

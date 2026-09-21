"""Тесты DQ-фреймворка."""

from __future__ import annotations

import pytest
from pyspark.sql import functions as F

from ecom.quality.checks import assert_no_fail_violations, evaluate_rules, split_valid_invalid
from ecom.quality.rules import RULES, Rule, as_expectations, rules_for
from ecom.transforms.silver import clean_order_items, clean_orders


def test_rule_rejects_unknown_severity():
    with pytest.raises(ValueError):
        Rule("x", "orders", "1=1", severity="maybe")


def test_rule_names_are_unique():
    names = [r.name for r in RULES]
    assert len(names) == len(set(names))


def test_as_expectations_matches_registry():
    drop = as_expectations("orders", "drop")
    assert set(drop) == {r.name for r in rules_for("orders", ("drop",))}
    assert drop["orders_customer_known"] == "customer_id IS NOT NULL"


def test_evaluate_rules_counts_failures(orders_raw):
    cleaned = clean_orders(orders_raw)
    metrics = {r["rule_name"]: r for r in evaluate_rules(cleaned, "orders").collect()}

    assert metrics["orders_customer_known"]["rows_failed"] == 1  # O2
    assert metrics["orders_ts_not_null"]["rows_failed"] == 1  # O4
    assert metrics["orders_status_known"]["rows_failed"] == 1  # O3
    assert metrics["orders_id_not_null"]["passed"] is True


def test_evaluate_rules_reports_rate(orders_raw):
    cleaned = clean_orders(orders_raw)
    row = next(
        r for r in evaluate_rules(cleaned, "orders").collect() if r["rule_name"] == "orders_customer_known"
    )
    assert row["rows_total"] == 4
    assert row["failure_rate"] == pytest.approx(0.25)


def test_split_sends_broken_rows_to_quarantine(orders_raw):
    cleaned = clean_orders(orders_raw)
    valid, invalid = split_valid_invalid(cleaned, "orders")

    assert valid.count() == 2, "остаются только O1 и O3"
    assert invalid.count() == 2

    reasons = {r["order_id"]: set(r["_dq_failed_rules"]) for r in invalid.collect()}
    assert reasons["O2"] == {"orders_customer_known"}
    assert "orders_ts_not_null" in reasons["O4"]


def test_warn_rules_do_not_quarantine(orders_raw):
    """O3 нарушает только warn-правило — он обязан остаться в валидных."""
    valid, _ = split_valid_invalid(clean_orders(orders_raw), "orders")
    assert "O3" in {r["order_id"] for r in valid.collect()}


def test_items_quarantine(order_items_raw):
    valid, invalid = split_valid_invalid(clean_order_items(order_items_raw), "order_items")

    assert valid.count() == 2  # OI1 и OI4
    reasons = {r["order_item_id"]: set(r["_dq_failed_rules"]) for r in invalid.collect()}
    assert reasons["OI2"] == {"items_quantity_positive"}
    assert reasons["OI3"] == {"items_price_positive"}


def test_assert_no_fail_violations_raises(orders_raw):
    broken = clean_orders(orders_raw).withColumn("order_id", F.lit(None).cast("string"))
    metrics = evaluate_rules(broken, "orders")
    with pytest.raises(RuntimeError, match="DQ blocking failures"):
        assert_no_fail_violations(metrics)


def test_assert_no_fail_violations_passes(orders_raw):
    metrics = evaluate_rules(clean_orders(orders_raw), "orders")
    assert_no_fail_violations(metrics)  # не должно бросить

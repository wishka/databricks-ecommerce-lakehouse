"""Декларативные правила качества данных.

Правило — это SQL-выражение, которое должно быть TRUE для валидной строки.
Ровно та же семантика, что у expectations в Lakeflow Declarative Pipelines,
поэтому один и тот же реестр используется и в batch-пайплайне (Part 1/5),
и в LDP (Part 2) — см. `pipelines/ecommerce_ldp/01_silver.py`.

severity:
  * `fail` — падаем, дальше считать нельзя (нарушена целостность);
  * `drop` — строку в карантин, остальное считаем;
  * `warn` — только метрика в `ops.dq_results`.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Rule:
    name: str
    dataset: str
    expression: str
    severity: str = "drop"
    description: str = ""

    def __post_init__(self) -> None:
        if self.severity not in {"fail", "drop", "warn"}:
            raise ValueError(f"unknown severity: {self.severity}")


RULES: tuple[Rule, ...] = (
    # ---------------- orders ----------------
    Rule(
        "orders_id_not_null",
        "orders",
        "order_id IS NOT NULL AND length(order_id) > 0",
        "fail",
        "Без order_id строка бессмысленна и ломает дедуп",
    ),
    Rule(
        "orders_customer_known",
        "orders",
        "customer_id IS NOT NULL",
        "drop",
        "Заказ без клиента не попадает в клиентские витрины",
    ),
    Rule(
        "orders_ts_not_null",
        "orders",
        "order_ts IS NOT NULL",
        "drop",
        "Не распарсился timestamp",
    ),
    Rule(
        "orders_ts_not_in_future",
        "orders",
        "order_ts <= current_timestamp() + INTERVAL 1 DAY",
        "drop",
        "Дата заказа из будущего — сбой источника",
    ),
    Rule(
        "orders_status_known",
        "orders",
        "status <> 'unknown'",
        "warn",
        "Неизвестный статус: считаем, но следим за долей",
    ),
    Rule(
        "orders_discount_range",
        "orders",
        "discount_pct BETWEEN 0 AND 0.9",
        "warn",
        "Подозрительная скидка",
    ),
    # ---------------- order_items ----------------
    Rule(
        "items_quantity_positive",
        "order_items",
        "quantity > 0",
        "drop",
        "Нулевое/отрицательное количество",
    ),
    Rule(
        "items_price_positive",
        "order_items",
        "unit_price > 0",
        "drop",
        "Отрицательная цена",
    ),
    Rule(
        "items_order_ref",
        "order_items",
        "order_id IS NOT NULL",
        "fail",
        "Позиция без заказа",
    ),
    Rule(
        "items_amount_consistent",
        "order_items",
        "abs(line_amount - quantity * unit_price) < 0.01",
        "warn",
        "Сумма строки не сходится с qty * price",
    ),
    # ---------------- customers ----------------
    Rule(
        "customers_id_not_null",
        "customers",
        "customer_id IS NOT NULL",
        "fail",
        "",
    ),
    Rule(
        "customers_email_valid",
        "customers",
        "is_email_valid",
        "warn",
        "Битый email — не рассылаем, но клиента держим",
    ),
    Rule(
        "customers_signup_past",
        "customers",
        "signup_date <= current_date()",
        "warn",
        "",
    ),
    # ---------------- clickstream ----------------
    Rule(
        "events_id_not_null",
        "clickstream",
        "event_id IS NOT NULL",
        "fail",
        "",
    ),
    Rule(
        "events_ts_not_null",
        "clickstream",
        "event_ts IS NOT NULL",
        "drop",
        "",
    ),
    Rule(
        "events_type_known",
        "clickstream",
        "event_type IN ('page_view','product_view','add_to_cart','remove_from_cart','checkout')",
        "drop",
        "",
    ),
)


def rules_for(dataset: str, severities: tuple[str, ...] | None = None) -> tuple[Rule, ...]:
    selected = tuple(r for r in RULES if r.dataset == dataset)
    if severities:
        selected = tuple(r for r in selected if r.severity in severities)
    return selected


def as_expectations(dataset: str, severity: str) -> dict[str, str]:
    """Реестр в формате, который принимают `@dp.expect_all*` из LDP."""
    return {r.name: r.expression for r in rules_for(dataset, (severity,))}

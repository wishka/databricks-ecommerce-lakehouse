"""Схемы «сырых» датасетов.

Bronze читается Auto Loader'ом со схемой, выведенной автоматически,
но для тестов и для явного `spark.createDataFrame` нужны типы.
Обратите внимание: в raw всё, что может прийти грязным, объявлено строкой —
приведение типов происходит в Silver.
"""

from __future__ import annotations

ORDER_FIELDS = [
    "order_id",
    "customer_id",
    "order_ts",
    "status",
    "channel",
    "currency",
    "shipping_country",
    "discount_pct",
]

ORDER_ITEM_FIELDS = [
    "order_item_id",
    "order_id",
    "product_id",
    "quantity",
    "unit_price",
]

CUSTOMER_FIELDS = [
    "customer_id",
    "full_name",
    "email",
    "phone",
    "country",
    "city",
    "segment",
    "signup_date",
    "updated_at",
]

PRODUCT_FIELDS = [
    "product_id",
    "product_name",
    "category",
    "subcategory",
    "brand",
    "list_price",
    "is_active",
    "updated_at",
]

CLICK_FIELDS = [
    "event_id",
    "session_id",
    "customer_id",
    "event_ts",
    "event_type",
    "product_id",
    "page_url",
    "device",
]


def spark_schema(fields: list[str]) -> str:
    """DDL-строка «всё строками» — так читаем raw без потерь."""
    return ", ".join(f"{f} STRING" for f in fields)

"""Bronze → Silver: типизация, дедупликация, нормализация.

Каждая функция принимает и возвращает DataFrame — поэтому тестируется локально
без Databricks (см. `tests/test_transforms_silver.py`).

Правило проекта: Silver ничего не отбрасывает молча. Некорректные строки
помечаются в `ecom.quality` и уезжают в карантин, а не исчезают.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

VALID_ORDER_STATUSES = ["created", "paid", "shipped", "delivered", "cancelled", "returned"]
REVENUE_STATUSES = ["paid", "shipped", "delivered"]


def _dedupe(df: DataFrame, keys: list[str], order_by: list) -> DataFrame:
    """Оставить по одной строке на ключ — последнюю по `order_by`.

    Именно так убирают ретраи продюсера: `dropDuplicates` без порядка
    недетерминирован, `row_number` — детерминирован.
    """
    window = Window.partitionBy(*keys).orderBy(*order_by)
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def clean_orders(bronze: DataFrame) -> DataFrame:
    """Заказы: типы, дедуп по order_id, нормализация статуса и валюты."""
    typed = (
        bronze.withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("customer_id", F.nullif(F.trim(F.col("customer_id")), F.lit("")))
        .withColumn("order_ts", F.to_timestamp("order_ts"))
        .withColumn("status", F.lower(F.trim(F.col("status"))))
        .withColumn("channel", F.lower(F.trim(F.col("channel"))))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("shipping_country", F.upper(F.trim(F.col("shipping_country"))))
        .withColumn("discount_pct", F.col("discount_pct").cast(T.DoubleType()))
    )

    normalized = (
        typed.withColumn(
            "status",
            F.when(F.col("status").isin(VALID_ORDER_STATUSES), F.col("status")).otherwise(
                F.lit("unknown")
            ),
        )
        .withColumn("order_date", F.to_date("order_ts"))
        .withColumn("is_revenue", F.col("status").isin(REVENUE_STATUSES))
    )

    order_by = [F.col("_ingested_at").desc()] if "_ingested_at" in normalized.columns else [
        F.col("order_ts").desc()
    ]
    return _dedupe(normalized, ["order_id"], order_by)


def clean_order_items(bronze: DataFrame) -> DataFrame:
    """Позиции заказа: типы, дедуп, расчёт суммы строки."""
    typed = (
        bronze.withColumn("order_item_id", F.trim(F.col("order_item_id")))
        .withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("quantity", F.col("quantity").cast(T.IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(T.DecimalType(12, 2)))
    )
    with_amount = typed.withColumn(
        "line_amount",
        (F.col("quantity") * F.col("unit_price")).cast(T.DecimalType(14, 2)),
    )
    order_by = [F.col("_ingested_at").desc()] if "_ingested_at" in with_amount.columns else [
        F.col("order_item_id").asc()
    ]
    return _dedupe(with_amount, ["order_item_id"], order_by)


def clean_customers(bronze: DataFrame) -> DataFrame:
    """Клиенты: типы, нормализация email, флаг валидности email.

    Дедуп по (customer_id, updated_at) — в CDC-фиде может прийти ретрай
    того же снимка.
    """
    typed = (
        bronze.withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("full_name", F.trim(F.col("full_name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .withColumn("city", F.initcap(F.trim(F.col("city"))))
        .withColumn("segment", F.lower(F.trim(F.col("segment"))))
        .withColumn("signup_date", F.to_date("signup_date"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )
    with_flags = typed.withColumn(
        "is_email_valid",
        F.col("email").rlike(r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"),
    )
    return _dedupe(with_flags, ["customer_id", "updated_at"], [F.col("email").asc()])


def clean_products(bronze: DataFrame) -> DataFrame:
    typed = (
        bronze.withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("category", F.initcap(F.trim(F.col("category"))))
        .withColumn("subcategory", F.initcap(F.trim(F.col("subcategory"))))
        .withColumn("brand", F.trim(F.col("brand")))
        .withColumn("list_price", F.col("list_price").cast(T.DecimalType(12, 2)))
        .withColumn("is_active", F.col("is_active").cast(T.BooleanType()))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )
    return _dedupe(typed, ["product_id"], [F.col("updated_at").desc()])


def clean_clickstream(bronze: DataFrame) -> DataFrame:
    """Клик-стрим: типы + производные поля для сессионной аналитики.

    Работает и на batch-, и на streaming-DataFrame — здесь нет действий,
    требующих полного набора данных.
    """
    return (
        bronze.withColumn("event_id", F.trim(F.col("event_id")))
        .withColumn("session_id", F.trim(F.col("session_id")))
        .withColumn("customer_id", F.nullif(F.trim(F.col("customer_id")), F.lit("")))
        .withColumn("event_ts", F.to_timestamp("event_ts"))
        .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
        .withColumn("product_id", F.nullif(F.trim(F.col("product_id")), F.lit("")))
        .withColumn("device", F.lower(F.trim(F.col("device"))))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("is_conversion", F.col("event_type") == F.lit("checkout"))
    )

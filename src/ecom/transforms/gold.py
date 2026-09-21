"""Silver → Gold: звезда и аналитические витрины.

Все функции идемпотентны и не зависят от того, где лежат данные, —
на вход приходят DataFrame'ы, на выход уходит DataFrame.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_dim_date(spark: SparkSession, start: date, end: date) -> DataFrame:
    """Календарь. Строится через sequence — без питоновских циклов."""
    df = spark.sql(
        f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'), interval 1 day)) AS date_key"
    )
    return (
        df.withColumn("year", F.year("date_key"))
        .withColumn("quarter", F.quarter("date_key"))
        .withColumn("month", F.month("date_key"))
        .withColumn("day", F.dayofmonth("date_key"))
        .withColumn("week_of_year", F.weekofyear("date_key"))
        .withColumn("day_of_week", F.dayofweek("date_key"))
        .withColumn("day_name", F.date_format("date_key", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("date_key").isin([1, 7]))
        .withColumn("month_start", F.trunc("date_key", "month"))
    )


def build_dim_customer(customers_current: DataFrame) -> DataFrame:
    """Дименсия клиентов из актуального среза SCD2.

    Суррогатный ключ — детерминированный хэш, чтобы пересчёт витрины
    не ломал ссылки в факте.
    """
    return customers_current.select(
        F.sha2(F.col("customer_id"), 256).substr(1, 16).alias("customer_sk"),
        "customer_id",
        "full_name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        F.datediff(F.current_date(), F.col("signup_date")).alias("tenure_days"),
    )


def build_dim_product(products: DataFrame) -> DataFrame:
    return products.select(
        F.sha2(F.col("product_id"), 256).substr(1, 16).alias("product_sk"),
        "product_id",
        "product_name",
        "category",
        "subcategory",
        "brand",
        "list_price",
        "is_active",
    )


def build_fct_order_items(
    orders: DataFrame,
    order_items: DataFrame,
    products: DataFrame,
) -> DataFrame:
    """Факт на уровне позиции заказа.

    `products` маленький — помечаем broadcast явно, чтобы не полагаться
    только на AQE (и чтобы это было видно в плане).
    """
    o = orders.select(
        "order_id",
        "customer_id",
        "order_ts",
        "order_date",
        "status",
        "channel",
        "currency",
        "shipping_country",
        "discount_pct",
        "is_revenue",
    )
    p = products.select("product_id", "category", "subcategory", "brand", "list_price")

    joined = (
        order_items.join(o, on="order_id", how="inner")
        .join(F.broadcast(p), on="product_id", how="left")
        .withColumn(
            "gross_amount", F.col("line_amount").cast(T.DecimalType(14, 2))
        )
        .withColumn(
            "discount_amount",
            (F.col("line_amount") * F.col("discount_pct")).cast(T.DecimalType(14, 2)),
        )
        .withColumn(
            "net_amount",
            (F.col("gross_amount") - F.col("discount_amount")).cast(T.DecimalType(14, 2)),
        )
    )

    return joined.select(
        "order_item_id",
        "order_id",
        F.sha2(F.col("customer_id"), 256).substr(1, 16).alias("customer_sk"),
        F.sha2(F.col("product_id"), 256).substr(1, 16).alias("product_sk"),
        "customer_id",
        "product_id",
        "order_date",
        "order_ts",
        "status",
        "channel",
        "shipping_country",
        "category",
        "subcategory",
        "brand",
        "quantity",
        "unit_price",
        "gross_amount",
        "discount_amount",
        "net_amount",
        "is_revenue",
    )


def build_daily_sales(fct: DataFrame) -> DataFrame:
    """Витрина продаж по дню/каналу/категории + скользящее среднее за 7 дней."""
    agg = (
        fct.where(F.col("is_revenue"))
        .groupBy("order_date", "channel", "category")
        .agg(
            F.countDistinct("order_id").alias("orders_cnt"),
            F.countDistinct("customer_id").alias("customers_cnt"),
            F.sum("quantity").alias("units"),
            F.sum("net_amount").alias("net_revenue"),
            F.sum("discount_amount").alias("discount_total"),
        )
        .withColumn(
            "avg_order_value",
            (F.col("net_revenue") / F.col("orders_cnt")).cast(T.DecimalType(14, 2)),
        )
    )

    w = (
        Window.partitionBy("channel", "category")
        .orderBy(F.col("order_date").cast("long"))
        .rangeBetween(-6 * 86400, 0)
    )
    return agg.withColumn(
        "net_revenue_7d_avg",
        F.avg("net_revenue").over(w).cast(T.DecimalType(14, 2)),
    )


def build_customer_rfm(fct: DataFrame, as_of: date | None = None) -> DataFrame:
    """RFM-сегментация: recency / frequency / monetary → квинтили → сегмент."""
    as_of_col = F.lit(as_of.isoformat()).cast("date") if as_of else F.current_date()

    base = (
        fct.where(F.col("is_revenue") & F.col("customer_id").isNotNull())
        .groupBy("customer_id")
        .agg(
            F.max("order_date").alias("last_order_date"),
            F.countDistinct("order_id").alias("frequency"),
            F.sum("net_amount").alias("monetary"),
        )
        .withColumn("recency_days", F.datediff(as_of_col, F.col("last_order_date")))
    )

    # ntile на всей выборке: recency — чем меньше, тем лучше, поэтому desc
    r_w = Window.orderBy(F.col("recency_days").desc())
    f_w = Window.orderBy(F.col("frequency").asc())
    m_w = Window.orderBy(F.col("monetary").asc())

    scored = (
        base.withColumn("r_score", F.ntile(5).over(r_w))
        .withColumn("f_score", F.ntile(5).over(f_w))
        .withColumn("m_score", F.ntile(5).over(m_w))
        .withColumn("rfm_score", F.col("r_score") + F.col("f_score") + F.col("m_score"))
    )

    return scored.withColumn(
        "segment",
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4), F.lit("champions"))
        .when((F.col("r_score") >= 4) & (F.col("f_score") < 4), F.lit("new_or_promising"))
        .when((F.col("r_score") <= 2) & (F.col("m_score") >= 4), F.lit("at_risk_high_value"))
        .when(F.col("r_score") <= 2, F.lit("hibernating"))
        .otherwise(F.lit("loyal")),
    )


def build_product_performance(fct: DataFrame, top_n: int = 10) -> DataFrame:
    """Топ-N товаров внутри каждой категории по выручке."""
    agg = (
        fct.where(F.col("is_revenue"))
        .groupBy("category", "product_id", "brand")
        .agg(
            F.sum("net_amount").alias("net_revenue"),
            F.sum("quantity").alias("units"),
            F.countDistinct("order_id").alias("orders_cnt"),
        )
    )
    w = Window.partitionBy("category").orderBy(F.col("net_revenue").desc())
    return (
        agg.withColumn("rank_in_category", F.dense_rank().over(w))
        .where(F.col("rank_in_category") <= top_n)
    )


def build_sessions(clickstream: DataFrame) -> DataFrame:
    """Сессионная витрина из клик-стрима (batch-версия; стрим — в Part 3)."""
    return (
        clickstream.groupBy("session_id", "event_date", "device")
        .agg(
            F.min("event_ts").alias("session_start"),
            F.max("event_ts").alias("session_end"),
            F.count("*").alias("events_cnt"),
            F.countDistinct("product_id").alias("products_viewed"),
            F.max(F.col("is_conversion").cast("int")).alias("converted"),
            F.first("customer_id", ignorenulls=True).alias("customer_id"),
        )
        .withColumn(
            "duration_sec",
            F.col("session_end").cast("long") - F.col("session_start").cast("long"),
        )
    )

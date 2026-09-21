"""Единая точка правды по именам каталогов, схем, таблиц и путей.

Каталог берётся из (в порядке приоритета):
1. явного аргумента,
2. Spark conf `ecom.catalog` (его выставляют job parameters / bundle variables),
3. переменной окружения `ECOM_CATALOG`,
4. дефолта `ecom_dev`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_CATALOG = "ecom_dev"

SCHEMAS = ("raw", "bronze", "silver", "gold", "ops", "ml", "ldp")

#: Имя managed volume, куда складываются исходные файлы.
LANDING_VOLUME = "landing"


def resolve_catalog(catalog: str | None = None, spark=None) -> str:
    if catalog:
        return catalog
    if spark is not None:
        try:
            from_conf = spark.conf.get("ecom.catalog", "")
        except Exception:
            from_conf = ""
        if from_conf:
            return from_conf
    return os.environ.get("ECOM_CATALOG", DEFAULT_CATALOG)


@dataclass(frozen=True)
class Config:
    """Резолвер полных имён. Всё, что пишется в UC, проходит через него."""

    catalog: str = DEFAULT_CATALOG

    # ---------- generic helpers ----------

    def schema(self, name: str) -> str:
        return f"{self.catalog}.{name}"

    def table(self, schema: str, name: str) -> str:
        return f"{self.catalog}.{schema}.{name}"

    def volume_root(self) -> str:
        return f"/Volumes/{self.catalog}/raw/{LANDING_VOLUME}"

    def landing_path(self, dataset: str) -> str:
        """Каталог с входящими файлами конкретного датасета."""
        return f"{self.volume_root()}/{dataset}"

    def checkpoint_path(self, stream: str) -> str:
        """Чекпоинты стримов живут в том же volume, но в служебной ветке."""
        return f"{self.volume_root()}/_checkpoints/{stream}"

    def schema_location(self, stream: str) -> str:
        """Место, где Auto Loader хранит выведенную схему."""
        return f"{self.volume_root()}/_schemas/{stream}"

    # ---------- bronze ----------

    @property
    def bronze_orders(self) -> str:
        return self.table("bronze", "orders_raw")

    @property
    def bronze_order_items(self) -> str:
        return self.table("bronze", "order_items_raw")

    @property
    def bronze_customers(self) -> str:
        return self.table("bronze", "customers_raw")

    @property
    def bronze_products(self) -> str:
        return self.table("bronze", "products_raw")

    @property
    def bronze_clickstream(self) -> str:
        return self.table("bronze", "clickstream_raw")

    # ---------- silver ----------

    @property
    def silver_orders(self) -> str:
        return self.table("silver", "orders")

    @property
    def silver_order_items(self) -> str:
        return self.table("silver", "order_items")

    @property
    def silver_products(self) -> str:
        return self.table("silver", "products")

    @property
    def silver_customers_scd2(self) -> str:
        return self.table("silver", "customers_scd2")

    @property
    def silver_customers_current(self) -> str:
        return self.table("silver", "customers_current")

    @property
    def silver_clickstream(self) -> str:
        return self.table("silver", "clickstream_events")

    # ---------- gold ----------

    @property
    def gold_dim_customer(self) -> str:
        return self.table("gold", "dim_customer")

    @property
    def gold_dim_product(self) -> str:
        return self.table("gold", "dim_product")

    @property
    def gold_dim_date(self) -> str:
        return self.table("gold", "dim_date")

    @property
    def gold_fct_order_items(self) -> str:
        return self.table("gold", "fct_order_items")

    @property
    def gold_daily_sales(self) -> str:
        return self.table("gold", "daily_sales")

    @property
    def gold_customer_rfm(self) -> str:
        return self.table("gold", "customer_rfm")

    @property
    def gold_sessions_5min(self) -> str:
        return self.table("gold", "sessions_5min")

    @property
    def gold_churn_scores(self) -> str:
        return self.table("gold", "customer_churn_scores")

    # ---------- ops / ml ----------

    @property
    def ops_dq_results(self) -> str:
        return self.table("ops", "dq_results")

    @property
    def ops_quarantine_orders(self) -> str:
        return self.table("ops", "quarantine_orders")

    @property
    def ops_pipeline_audit(self) -> str:
        return self.table("ops", "pipeline_audit")

    @property
    def ml_customer_features(self) -> str:
        return self.table("ml", "customer_features")

    @property
    def ml_churn_model(self) -> str:
        return self.table("ml", "churn_model")


def load_config(catalog: str | None = None, spark=None) -> Config:
    return Config(catalog=resolve_catalog(catalog, spark))

"""Тесты резолвинга каталога.

Регрессия: на serverless `spark.conf.get(key, default)` для незаданного
пользовательского ключа не возвращает дефолт, а бросает CONFIG_NOT_AVAILABLE.
Из-за этого падал `%run ../_bootstrap` в чистом воркспейсе.
"""

from __future__ import annotations

import pytest

from ecom.config import DEFAULT_CATALOG, Config, load_config, resolve_catalog
from ecom.utils.spark import conf_get


class _RaisingConf:
    """Имитация serverless: незаданный ключ бросает исключение."""

    class conf:
        @staticmethod
        def get(key):
            raise RuntimeError(f"[CONFIG_NOT_AVAILABLE] {key} is not available")


class _SetConf:
    class conf:
        @staticmethod
        def get(key):
            return "ecom_prod"


def test_conf_get_returns_default_when_key_raises():
    assert conf_get(_RaisingConf(), "ecom.catalog", "fallback") == "fallback"


def test_conf_get_returns_value_when_key_set():
    assert conf_get(_SetConf(), "ecom.catalog", "fallback") == "ecom_prod"


def test_conf_get_handles_none_spark():
    assert conf_get(None, "ecom.catalog", "fallback") == "fallback"


def test_resolve_catalog_survives_missing_conf():
    assert resolve_catalog(spark=_RaisingConf()) == DEFAULT_CATALOG


def test_resolve_catalog_reads_conf():
    assert resolve_catalog(spark=_SetConf()) == "ecom_prod"


def test_explicit_argument_wins():
    assert load_config("ecom_test", spark=_SetConf()).catalog == "ecom_test"


def test_env_var_used_when_no_spark(monkeypatch):
    monkeypatch.setenv("ECOM_CATALOG", "ecom_from_env")
    assert resolve_catalog() == "ecom_from_env"


@pytest.mark.parametrize(
    ("attr", "expected"),
    [
        ("bronze_orders", "ecom_dev.bronze.orders_raw"),
        ("silver_customers_scd2", "ecom_dev.silver.customers_scd2"),
        ("gold_fct_order_items", "ecom_dev.gold.fct_order_items"),
        ("ops_dq_results", "ecom_dev.ops.dq_results"),
        ("ml_churn_model", "ecom_dev.ml.churn_model"),
    ],
)
def test_table_names(attr, expected):
    assert getattr(Config(), attr) == expected


def test_volume_paths():
    cfg = Config(catalog="ecom_prod")
    assert cfg.volume_root() == "/Volumes/ecom_prod/raw/landing"
    assert cfg.landing_path("orders") == "/Volumes/ecom_prod/raw/landing/orders"
    assert cfg.checkpoint_path("s") == "/Volumes/ecom_prod/raw/landing/_checkpoints/s"

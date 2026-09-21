"""Тонкая обвязка над SparkSession.

В Databricks сессия уже есть — берём активную. Локально (в pytest) поднимаем
маленький Spark. Delta подключается через `delta-spark`, который на первом
запуске подтягивает jar'ы с Maven Central; если сети нет, откатываемся на
чистый Spark — трансформации тестируются и без Delta, а интеграционные тесты
SCD2 в этом случае пропускаются.
"""

from __future__ import annotations

import os
from typing import Any

_DELTA_CONF = {
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
}


def _base_builder(app_name: str):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )


def get_spark(app_name: str = "ecom-lakehouse") -> Any:
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        return active

    if os.environ.get("ECOM_DISABLE_DELTA") != "1":
        submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS")
        try:
            from delta import configure_spark_with_delta_pip

            builder = _base_builder(app_name)
            for key, value in _DELTA_CONF.items():
                builder = builder.config(key, value)
            return configure_spark_with_delta_pip(builder).getOrCreate()
        except Exception:
            # Нет delta-spark или не скачались jar'ы (офлайн / закрытый proxy).
            # Восстанавливаем окружение, чтобы --packages не мешал фолбэку.
            if submit_args is None:
                os.environ.pop("PYSPARK_SUBMIT_ARGS", None)
            else:
                os.environ["PYSPARK_SUBMIT_ARGS"] = submit_args

    return _base_builder(app_name).getOrCreate()


def delta_enabled(spark: Any) -> bool:
    """Умеет ли эта сессия в Delta. Проверяем делом, а не по конфигу."""
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS _delta_probe")
        spark.sql("CREATE TABLE IF NOT EXISTS _delta_probe.t (a INT) USING DELTA")
        spark.sql("DROP TABLE IF EXISTS _delta_probe.t")
        return True
    except Exception:
        return False


def table_exists(spark: Any, full_name: str) -> bool:
    try:
        return spark.catalog.tableExists(full_name)
    except Exception:
        return False


def sql_scalar(spark: Any, query: str) -> Any:
    """Выполнить запрос и вернуть первое значение первой строки (или None)."""
    rows = spark.sql(query).take(1)
    return rows[0][0] if rows else None

"""Исполнение DQ-правил: метрики + разделение на валидное и карантин."""

from __future__ import annotations

import datetime as dt
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ecom.quality.rules import Rule, rules_for

DQ_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
  run_id          STRING    NOT NULL,
  run_ts          TIMESTAMP NOT NULL,
  dataset         STRING    NOT NULL,
  rule_name       STRING    NOT NULL,
  severity        STRING    NOT NULL,
  expression      STRING,
  rows_total      BIGINT,
  rows_failed     BIGINT,
  failure_rate    DOUBLE,
  passed          BOOLEAN
)
USING DELTA
COMMENT 'Результаты проверок качества данных по запускам'
CLUSTER BY (dataset, run_ts)
"""

QUARANTINE_COMMENT = "Строки, не прошедшие DQ-правила severity=drop/fail"


def _failed_flag(rule: Rule):
    """NULL в выражении трактуем как провал — как это делает Delta CHECK."""
    return F.when(F.expr(rule.expression), F.lit(0)).otherwise(F.lit(1))


def evaluate_rules(
    df: DataFrame,
    dataset: str,
    run_id: str | None = None,
    run_ts: dt.datetime | None = None,
) -> DataFrame:
    """Посчитать все правила датасета одним проходом и вернуть метрики.

    Важно: агрегируем все правила в одном `agg`, а не циклом с `count()` —
    иначе получится N полных сканов таблицы.
    """
    rules = rules_for(dataset)
    if not rules:
        raise ValueError(f"нет правил для датасета {dataset!r}")

    run_id = run_id or str(uuid.uuid4())
    run_ts = run_ts or dt.datetime.now(dt.UTC).replace(tzinfo=None)

    aggs = [F.count(F.lit(1)).alias("rows_total")]
    aggs += [F.sum(_failed_flag(r)).alias(f"failed__{r.name}") for r in rules]
    metrics = df.agg(*aggs).collect()[0]

    spark = df.sparkSession
    rows = []
    total = int(metrics["rows_total"])
    for rule in rules:
        failed = int(metrics[f"failed__{rule.name}"] or 0)
        rows.append(
            (
                run_id,
                run_ts,
                dataset,
                rule.name,
                rule.severity,
                rule.expression,
                total,
                failed,
                (failed / total) if total else 0.0,
                failed == 0,
            )
        )

    return spark.createDataFrame(
        rows,
        schema=(
            "run_id string, run_ts timestamp, dataset string, rule_name string, "
            "severity string, expression string, rows_total bigint, rows_failed bigint, "
            "failure_rate double, passed boolean"
        ),
    )


def split_valid_invalid(df: DataFrame, dataset: str) -> tuple[DataFrame, DataFrame]:
    """Разделить датасет на «чистое» и «карантин».

    Карантин получает колонку `_dq_failed_rules` — массив нарушенных правил,
    по нему потом удобно разбирать инциденты.
    """
    blocking = rules_for(dataset, ("fail", "drop"))
    if not blocking:
        empty = df.limit(0).withColumn("_dq_failed_rules", F.array().cast("array<string>"))
        return df, empty

    failed_arr = F.array_compact(
        F.array(
            *[
                F.when(~F.expr(r.expression) | F.expr(r.expression).isNull(), F.lit(r.name))
                for r in blocking
            ]
        )
    )
    tagged = df.withColumn("_dq_failed_rules", failed_arr)

    valid = tagged.where(F.size("_dq_failed_rules") == 0).drop("_dq_failed_rules")
    invalid = tagged.where(F.size("_dq_failed_rules") > 0).withColumn(
        "_quarantined_at", F.current_timestamp()
    )
    return valid, invalid


def assert_no_fail_violations(metrics: DataFrame) -> None:
    """Остановить джобу, если нарушено правило severity='fail'."""
    bad = metrics.where((F.col("severity") == "fail") & (~F.col("passed"))).collect()
    if bad:
        details = ", ".join(f"{r['rule_name']}={r['rows_failed']}" for r in bad)
        raise RuntimeError(f"DQ blocking failures: {details}")

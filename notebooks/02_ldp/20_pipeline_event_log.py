# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2.5 — Разбор event log пайплайна
# MAGIC
# MAGIC Event log — это обычная Delta-таблица, в которой пайплайн пишет всё:
# MAGIC сколько строк записано, сколько отброшено по expectations, сколько
# MAGIC длился каждый flow, какие были ошибки.
# MAGIC
# MAGIC Начиная с Lakeflow, event log можно опубликовать в Unity Catalog — тогда
# MAGIC он доступен как таблица (`event_log` в конфиге пайплайна, см.
# MAGIC `resources/pipelines/ecommerce_ldp.yml`). Альтернатива — табличная функция
# MAGIC `event_log('<pipeline-id>')`.
# MAGIC
# MAGIC Это тот самый артефакт, который стоит показать на собеседовании:
# MAGIC «качество данных у меня не в комментариях, а в таблице с историей».

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821
dbutils.widgets.text("event_log_table", "ecom_dev.ops.ldp_event_log")  # noqa: F821

# COMMAND ----------

from ecom.config import Config

cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
EVENT_LOG = dbutils.widgets.get("event_log_table")  # noqa: F821
spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821

# COMMAND ----------

# MAGIC %md ## Структура: что вообще пишется в лог

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"""
        SELECT event_type, count(*) AS events
        FROM {EVENT_LOG}
        GROUP BY event_type ORDER BY events DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Метрики expectations
# MAGIC `details:flow_progress.data_quality.expectations` — массив по каждому правилу:
# MAGIC имя, датасет, сколько строк прошло и сколько нет.

# COMMAND ----------

spark.sql(  # noqa: F821
    f"""
    CREATE OR REPLACE TEMP VIEW expectation_metrics AS
    SELECT
      timestamp,
      origin.flow_name AS flow_name,
      exp.name         AS expectation_name,
      exp.dataset      AS dataset,
      exp.passed_records,
      exp.failed_records
    FROM {EVENT_LOG}
    LATERAL VIEW explode(
      from_json(
        details:flow_progress.data_quality.expectations,
        'array<struct<name:string, dataset:string, passed_records:bigint, failed_records:bigint>>'
      )
    ) t AS exp
    WHERE event_type = 'flow_progress'
      AND details:flow_progress.data_quality.expectations IS NOT NULL
    """
)

display(  # noqa: F821
    spark.sql(  # noqa: F821
        """
        SELECT
          dataset,
          expectation_name,
          sum(passed_records) AS passed,
          sum(failed_records) AS failed,
          round(100.0 * sum(failed_records) / nullif(sum(passed_records) + sum(failed_records), 0), 3)
            AS failed_pct
        FROM expectation_metrics
        GROUP BY dataset, expectation_name
        ORDER BY failed DESC
        """
    )
)

# COMMAND ----------

# MAGIC %md ## Производительность: сколько строк и времени на каждый flow

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"""
        SELECT
          origin.flow_name                                      AS flow_name,
          timestamp,
          details:flow_progress.status                          AS status,
          details:flow_progress.metrics.num_output_rows::bigint AS output_rows,
          details:flow_progress.metrics.backlog_bytes::bigint   AS backlog_bytes
        FROM {EVENT_LOG}
        WHERE event_type = 'flow_progress'
          AND details:flow_progress.metrics IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT 50
        """
    )
)

# COMMAND ----------

# MAGIC %md ## Ошибки и предупреждения

# COMMAND ----------

display(  # noqa: F821
    spark.sql(  # noqa: F821
        f"""
        SELECT timestamp, level, message, error
        FROM {EVENT_LOG}
        WHERE level IN ('WARN', 'ERROR')
        ORDER BY timestamp DESC
        LIMIT 30
        """
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Сохраняем метрики в ops
# MAGIC Чтобы отчёт по качеству был единым для batch- и LDP-веток проекта,
# MAGIC складываем результаты в ту же `ops.dq_results`.

# COMMAND ----------

spark.sql(  # noqa: F821
    f"""
    CREATE OR REPLACE TABLE {cfg.table("ops", "ldp_expectations")}
    COMMENT 'Метрики expectations из event log Lakeflow-пайплайна'
    AS SELECT * FROM expectation_metrics
    """
)
display(spark.table(cfg.table("ops", "ldp_expectations")).limit(20))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ### Дальше
# MAGIC Part 3 — `notebooks/03_streaming/30_clickstream_producer.py`

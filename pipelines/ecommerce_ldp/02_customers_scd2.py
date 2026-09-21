# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2.3 — AUTO CDC: SCD Type 1 и Type 2 «из коробки»
# MAGIC
# MAGIC Сравните с `src/ecom/transforms/scd.py` — там те же семь строк смысла
# MAGIC разворачиваются в MERGE с UNION-трюком, хэшами и ручным закрытием версий.
# MAGIC Здесь движок делает это сам, включая:
# MAGIC
# MAGIC * переупорядочивание событий, пришедших не по порядку (`sequence_by`);
# MAGIC * идемпотентность при перезапуске;
# MAGIC * `__START_AT` / `__END_AT` для SCD2.
# MAGIC
# MAGIC Схема цели объявляется явно: для SCD2 движок требует, чтобы `__START_AT` и
# MAGIC `__END_AT` были того же типа, что и колонка `sequence_by`.

# COMMAND ----------

from pyspark import pipelines as dp
from pyspark.sql import functions as F

EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Подготовка источника
# MAGIC Временное представление: чистим типы, но не агрегируем — упорядочивание
# MAGIC и дедуп по ключу возьмёт на себя AUTO CDC.


# COMMAND ----------


@dp.temporary_view(name="customers_cdc")
def customers_cdc():
    return (
        spark.readStream.table("bronze_customers")  # noqa: F821
        .select(
            F.trim("customer_id").alias("customer_id"),
            F.trim("full_name").alias("full_name"),
            F.lower(F.trim("email")).alias("email"),
            F.trim("phone").alias("phone"),
            F.upper(F.trim("country")).alias("country"),
            F.initcap(F.trim("city")).alias("city"),
            F.lower(F.trim("segment")).alias("segment"),
            F.to_date("signup_date").alias("signup_date"),
            F.to_timestamp("updated_at").alias("updated_at"),
        )
        .withColumn("is_email_valid", F.col("email").rlike(EMAIL_RE))
    )


# COMMAND ----------

# MAGIC %md ## SCD Type 2 — полная история изменений

# COMMAND ----------

dp.create_streaming_table(
    name="silver_customers_scd2",
    comment="История изменений клиентов (SCD Type 2)",
    table_properties={"quality": "silver"},
    expect_all_or_fail={"customer_id_not_null": "customer_id IS NOT NULL"},
)

dp.create_auto_cdc_flow(
    target="silver_customers_scd2",
    source="customers_cdc",
    keys=["customer_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type="2",
    # `updated_at` не кладём в целевую таблицу — он уже стал `__START_AT`
    except_column_list=["updated_at"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC Полезный соседний параметр — `track_history_except_column_list`:
# MAGIC он оставляет колонку в таблице, но её изменение **не** создаёт новую версию.
# MAGIC Так поступают с техническими полями вроде `phone` или `last_login_at`,
# MAGIC чтобы история не распухала от косметических правок.
# MAGIC Задавать вместе с `except_column_list` для одних и тех же колонок не нужно.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SCD Type 1 — только актуальный срез
# MAGIC Та же команда, `stored_as_scd_type="1"`. Нужна витринам, которым история не важна:
# MAGIC читать её дешевле, чем фильтровать SCD2 по `is_current`.

# COMMAND ----------

dp.create_streaming_table(
    name="silver_customers_current",
    comment="Актуальный срез клиентов (SCD Type 1)",
    table_properties={"quality": "silver"},
)

dp.create_auto_cdc_flow(
    target="silver_customers_current",
    source="customers_cdc",
    keys=["customer_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type="1",
    except_column_list=["updated_at"],
)

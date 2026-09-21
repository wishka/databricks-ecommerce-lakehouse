# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1.4 — Delta Lake вглубь
# MAGIC
# MAGIC Ноутбук-лаборатория: здесь не строится ничего нового, здесь щупаются
# MAGIC механизмы, о которых спрашивают на собеседованиях.
# MAGIC
# MAGIC 1. Транзакционный лог и `DESCRIBE HISTORY`
# MAGIC 2. Time travel: `VERSION AS OF` / `TIMESTAMP AS OF`, `RESTORE`
# MAGIC 3. `MERGE INTO`: upsert, дедуп, `WHEN NOT MATCHED BY SOURCE`
# MAGIC 4. Change Data Feed — инкрементальное чтение изменений
# MAGIC 5. `OPTIMIZE`, liquid clustering, `VACUUM`, статистика и data skipping
# MAGIC 6. `CLONE` — дешёвая копия для экспериментов
# MAGIC 7. Схема: `mergeSchema`, `overwriteSchema`, column mapping, rename

# COMMAND ----------

# MAGIC %run ../_bootstrap

# COMMAND ----------

dbutils.widgets.text("catalog", "ecom_dev")  # noqa: F821

# COMMAND ----------

from ecom.config import Config

cfg = Config(catalog=dbutils.widgets.get("catalog"))  # noqa: F821
spark.sql(f"USE CATALOG {cfg.catalog}")  # noqa: F821
SANDBOX = cfg.table("ops", "delta_sandbox")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Транзакционный лог
# MAGIC Каждая операция — новая версия. `DESCRIBE HISTORY` показывает кто, что и
# MAGIC сколько файлов написал. Это первый инструмент при разборе «почему цифры разъехались».

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {cfg.gold_fct_order_items}"))  # noqa: F821

# COMMAND ----------

display(spark.sql(f"DESCRIBE DETAIL {cfg.gold_fct_order_items}"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Time travel
# MAGIC Сравниваем текущую версию с предыдущей — классический способ ответить
# MAGIC на вопрос «что изменилось со вчера».

# COMMAND ----------

versions = [r["version"] for r in spark.sql(f"DESCRIBE HISTORY {cfg.silver_orders}").collect()]  # noqa: F821
print("доступные версии:", versions[:10])

if len(versions) > 1:
    current = spark.table(cfg.silver_orders)  # noqa: F821
    previous = spark.read.option("versionAsOf", versions[1]).table(cfg.silver_orders)  # noqa: F821
    print("сейчас:", current.count(), "| было:", previous.count())
    display(current.select("order_id").exceptAll(previous.select("order_id")).limit(10))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC `RESTORE` откатывает таблицу на версию назад — и сам становится новой версией,
# MAGIC то есть откат тоже обратим.
# MAGIC
# MAGIC ```sql
# MAGIC RESTORE TABLE silver.orders TO VERSION AS OF 1;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. MERGE INTO на песочнице
# MAGIC Инкрементальный upsert: новые заказы вставляем, изменившиеся обновляем,
# MAGIC пропавшие в источнике — помечаем удалёнными (soft delete).

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {SANDBOX}")  # noqa: F821
spark.sql(  # noqa: F821
    f"""
    CREATE TABLE {SANDBOX}
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    AS SELECT order_id, customer_id, status, order_ts, false AS is_deleted
       FROM {cfg.silver_orders} LIMIT 1000
    """
)
spark.sql(f"SELECT count(*) FROM {SANDBOX}").show()  # noqa: F821

# COMMAND ----------

spark.sql(  # noqa: F821
    f"""
    CREATE OR REPLACE TEMP VIEW orders_updates AS
    SELECT order_id, customer_id, 'delivered' AS status, order_ts
    FROM {SANDBOX} LIMIT 200
    UNION ALL
    SELECT order_id, customer_id, status, order_ts
    FROM {cfg.silver_orders}
    WHERE order_id NOT IN (SELECT order_id FROM {SANDBOX})
    LIMIT 100
    """
)

spark.sql(  # noqa: F821
    f"""
    MERGE INTO {SANDBOX} AS t
    USING orders_updates AS s
    ON t.order_id = s.order_id
    WHEN MATCHED AND t.status <> s.status THEN UPDATE SET t.status = s.status
    WHEN NOT MATCHED THEN INSERT (order_id, customer_id, status, order_ts, is_deleted)
                        VALUES (s.order_id, s.customer_id, s.status, s.order_ts, false)
    WHEN NOT MATCHED BY SOURCE THEN UPDATE SET t.is_deleted = true
    """
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC `WHEN NOT MATCHED BY SOURCE` — то, ради чего стоит знать современный синтаксис:
# MAGIC раньше soft delete требовал отдельного anti-join'а.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Change Data Feed
# MAGIC CDF отдаёт не снимок, а поток изменений: `insert`, `update_preimage`,
# MAGIC `update_postimage`, `delete`. Это основа инкрементальных Gold-витрин.

# COMMAND ----------

display(  # noqa: F821
    spark.read.format("delta")  # noqa: F821
    .option("readChangeFeed", "true")
    .option("startingVersion", 1)
    .table(SANDBOX)
    .orderBy("_commit_version", "_change_type")
    .limit(20)
)

# COMMAND ----------

# MAGIC %md ## 5. OPTIMIZE, liquid clustering, VACUUM

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {cfg.silver_orders}"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC **Liquid clustering vs партиционирование.**
# MAGIC Партиции — это физические директории: ошиблись с гранулярностью →
# MAGIC миллионы мелких файлов, и переразбить можно только переписав таблицу.
# MAGIC Liquid clustering задаёт ключи логически, ключи можно менять на лету:
# MAGIC
# MAGIC ```sql
# MAGIC ALTER TABLE silver.orders CLUSTER BY (order_date, channel);
# MAGIC ```
# MAGIC
# MAGIC Практическое правило: партиционировать только если таблица большая
# MAGIC и партиция даёт ≥ 1 ГБ данных; во всех остальных случаях — liquid clustering.

# COMMAND ----------

spark.sql(f"ALTER TABLE {cfg.silver_orders} CLUSTER BY (order_date, channel)")  # noqa: F821
display(spark.sql(f"DESCRIBE DETAIL {cfg.silver_orders}"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data skipping
# MAGIC Delta хранит min/max по первым 32 колонкам. Если фильтр попадает на
# MAGIC кластеризованную колонку — читается меньше файлов. Сравните
# MAGIC `number of files read` в плане с фильтром и без.

# COMMAND ----------

spark.sql(  # noqa: F821
    f"SELECT count(*) FROM {cfg.silver_orders} WHERE order_date = (SELECT max(order_date) FROM {cfg.silver_orders})"
).explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC `VACUUM` удаляет файлы, на которые больше не ссылается лог.
# MAGIC Порог по умолчанию — 7 дней: короче нельзя, иначе сломается time travel
# MAGIC и читающие запросы. На проде не ставьте `RETAIN 0 HOURS`.
# MAGIC
# MAGIC ```sql
# MAGIC VACUUM silver.orders DRY RUN;
# MAGIC ```

# COMMAND ----------

display(spark.sql(f"VACUUM {SANDBOX} DRY RUN"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. CLONE
# MAGIC `SHALLOW CLONE` копирует только метаданные — мгновенно и почти бесплатно.
# MAGIC Идеальный способ дать аналитику «копию прода» для экспериментов.

# COMMAND ----------

clone_table = cfg.table("ops", "orders_shallow_clone")
spark.sql(f"CREATE OR REPLACE TABLE {clone_table} SHALLOW CLONE {cfg.silver_orders}")  # noqa: F821
display(spark.sql(f"DESCRIBE DETAIL {clone_table}"))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Эволюция схемы
# MAGIC * `mergeSchema` — добавить новые колонки при записи;
# MAGIC * `overwriteSchema` — полностью заменить схему (только с `overwrite`);
# MAGIC * column mapping — переименование и удаление колонок без переписывания файлов.

# COMMAND ----------

spark.sql(  # noqa: F821
    f"ALTER TABLE {SANDBOX} SET TBLPROPERTIES ("
    "  'delta.columnMapping.mode' = 'name',"
    "  'delta.minReaderVersion' = '2',"
    "  'delta.minWriterVersion' = '5')"
)
spark.sql(f"ALTER TABLE {SANDBOX} RENAME COLUMN status TO order_status")  # noqa: F821
display(spark.table(SANDBOX).limit(5))  # noqa: F821

# COMMAND ----------

# MAGIC %md ## Уборка песочницы

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {SANDBOX}")  # noqa: F821
spark.sql(f"DROP TABLE IF EXISTS {clone_table}")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ### Дальше
# MAGIC Part 2 — `pipelines/ecommerce_ldp/`: тот же медальон, но декларативно.

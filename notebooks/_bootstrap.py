# Databricks notebook source
# MAGIC %md
# MAGIC # Bootstrap: сделать пакет `ecom` импортируемым из ноутбука
# MAGIC
# MAGIC Ноутбуки лежат в `notebooks/<part>/`, код — в `src/ecom/`.
# MAGIC В Git folder рабочая директория ноутбука = его папка, поэтому поднимаемся
# MAGIC вверх до корня репозитория и добавляем `src` в `sys.path`.
# MAGIC
# MAGIC Подключается из любого ноутбука одной строкой:
# MAGIC ```python
# MAGIC %run ../_bootstrap
# MAGIC ```

# COMMAND ----------

import pathlib
import sys


def _add_src_to_path() -> str:
    here = pathlib.Path.cwd().resolve()
    for candidate in [here, *here.parents]:
        src = candidate / "src"
        if (src / "ecom" / "__init__.py").exists():
            if str(src) not in sys.path:
                sys.path.insert(0, str(src))
            return str(src)
    raise RuntimeError(
        "Не нашёл src/ecom — проверьте, что ноутбук запущен из Git folder с этим репозиторием"
    )


ECOM_SRC = _add_src_to_path()
print(f"ecom package path: {ECOM_SRC}")

# COMMAND ----------

from ecom.config import load_config  # noqa: E402

CFG = load_config(spark=spark)  # noqa: F821
print(f"catalog: {CFG.catalog}")

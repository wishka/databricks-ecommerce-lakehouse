# Выкладываем проект на GitHub

## 1. Создать репозиторий

На github.com: **New repository** → имя `databricks-ecommerce-lakehouse`,
public, **без** README/.gitignore/лицензии (они уже есть в проекте).

## 2. Первый коммит

```bash
cd databricks-ecommerce-lakehouse

git init -b main
git add .
git commit -m "Part 0-6: e-commerce lakehouse on Databricks"

git remote add origin git@github.com:<username>/databricks-ecommerce-lakehouse.git
git push -u origin main
```

Перед первым push убедитесь, что токенов в коде нет:

```bash
git grep -nE "dapi[a-f0-9]{10,}|DATABRICKS_TOKEN\s*=\s*[\"']" || echo "чисто"
```

## 3. Секреты для CI

**Settings → Secrets and variables → Actions → New repository secret**:

| Имя | Значение |
|---|---|
| `DATABRICKS_HOST` | `https://dbc-ad3f947c-7375.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | PAT из Databricks (Settings → Developer → Access tokens) |

## 4. Защитить main

**Settings → Branches → Add rule** для `main`:
require pull request, require status checks (`lint-and-test`).
Так CI перестаёт быть декорацией.

## 5. Подключить репозиторий в Databricks

1. **Settings → Linked accounts → Git integration**: GitHub + PAT с правом `repo`.
2. **Workspace → Repos → Add repo** → URL репозитория.
3. Ветка `main` для чтения, работа — в feature-ветках прямо из UI.

## Рабочий цикл

```bash
git switch -c feature/part3-streaming
# правки в src/, notebooks/, resources/
pytest -q && ruff check .
git commit -am "Part 3: windowed aggregations with watermark"
git push -u origin feature/part3-streaming
# PR → CI → merge → deploy.yml разворачивает бандл в dev
```

## Как оформить репозиторий, чтобы его читали

* **README** — архитектура одной картинкой и что именно вы освоили,
  а не пересказ документации Databricks.
* **Скриншоты** в `docs/img/`: граф LDP-пайплайна, дашборд, lineage
  в Catalog Explorer, зелёный CI. Это первое, что смотрят.
* **Коммиты по частям** — история показывает ход работы лучше, чем
  один коммит «initial commit» на 5000 строк.
* **Чек-лист** `docs/feature-checklist.md` с отмеченными пунктами —
  готовый ответ на вопрос «что вы умеете в Databricks».

## Что НЕ коммитить

`.gitignore` уже закрывает `.databricks/`, `.venv/`, `dist/`, `spark-warehouse/`,
`_checkpoints/`. Отдельно следите, чтобы в репозиторий не попали:
PAT и `~/.databrickscfg`, выгрузки реальных данных, `.env`.

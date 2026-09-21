# Part 4 — Jobs, Asset Bundles, CI/CD

**Время:** 3–4 часа. **Результат:** `databricks bundle deploy` создаёт все джобы
и пайплайн; PR прогоняет линт и тесты.

## Локальная подготовка

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

pytest -q          # ~40 тестов на локальном Spark
ruff check .
```

Нужна Java 17 — PySpark без неё не стартует.

## Databricks CLI и авторизация

```bash
# macOS
brew tap databricks/tap && brew install databricks

# везде
pip install databricks-cli
```

Free Edition: авторизация по personal access token
(**Settings → Developer → Access tokens → Generate new token**).

```bash
export DATABRICKS_HOST=https://dbc-ad3f947c-7375.cloud.databricks.com
export DATABRICKS_TOKEN=dapi...
databricks current-user me      # проверка
```

Токен в репозиторий не попадает никогда: локально — переменные окружения
или `~/.databrickscfg`, в CI — GitHub Secrets.

## Бандл

```bash
databricks bundle validate -t dev    # проверить конфиг и пути
databricks bundle deploy   -t dev    # залить файлы, создать джобы и пайплайн
databricks bundle run ecom_bootstrap        -t dev
databricks bundle run ecom_batch_medallion  -t dev
databricks bundle summary  -t dev    # ссылки на созданные ресурсы
databricks bundle destroy  -t dev    # снести всё, что бандл создал
```

Что даёт бандл:

* **Ресурсы как код.** Джоба, созданная в UI, живёт только в этом воркспейсе
  и исчезает вместе с ним. Джоба из бандла восстанавливается одной командой.
* **Окружения.** `-t dev` и `-t prod` подставляют разные значения `${var.catalog}`,
  код при этом один.
* **Режим development.** К именам добавляется `[dev <ваш логин>]`, расписания
  ставятся на паузу — параллельная работа нескольких человек не конфликтует.
* **Артефакты.** `artifacts.ecom_wheel` собирает пакет и делает его доступным
  задачам и пайплайну.

## Джобы

| Джоба | Что делает | Расписание |
|---|---|---|
| `ecom_bootstrap` | каталог + генерация данных | вручную |
| `ecom_batch_medallion` | bronze → silver → gold → DQ-отчёт | 03:30 UTC, PAUSED |
| `ecom_streaming` | продюсер + оконные агрегаты | continuous, PAUSED |
| `ecom_ml` | фичи → обучение → скоринг | понедельник 05:00 UTC, PAUSED |

Приёмы, на которые стоит посмотреть:

* `depends_on` строит граф — независимые задачи движок запускает параллельно;
* `run_if: AT_LEAST_ONE_SUCCESS` на DQ-отчёте: он выполняется даже если
  предыдущий шаг упал, иначе о падении не будет записи;
* `max_retries` + `min_retry_interval_millis` только там, где падение может быть
  временным; на шаге с DQ ретраи бессмысленны — данные не починятся сами;
* `max_concurrent_runs: 1` — витрины пишутся `overwrite`, параллельные запуски
  затирали бы друг друга;
* `health.rules` с `RUN_DURATION_SECONDS` — сигнал о «подвисшем» стриме.

Free Edition: не больше 5 одновременных задач на аккаунт — учитывайте,
если запустите несколько джоб разом.

## CI

`.github/workflows/ci.yml` на каждый PR:

1. `ruff check .`
2. `pytest` на локальном PySpark — без Databricks и без сети;
3. `databricks bundle validate -t dev` — ловит опечатки в YAML и
   несуществующие пути к ноутбукам.

`.github/workflows/deploy.yml` на push в `main`: собирает wheel,
деплоит бандл в `dev` и делает smoke-run медальона.

Секреты репозитория: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`
(**Settings → Secrets and variables → Actions**).

## Что тестируем и почему именно это

Тестируются модули `src/ecom`, а не ноутбуки: ноутбук — оркестрация,
логика живёт в пакете. Покрыто:

* `test_transforms_silver.py` — дедуп детерминирован, статусы нормализуются,
  битые строки доезжают до DQ, а не исчезают;
* `test_transforms_gold.py` — скидки, исключение отменённых заказов из выручки,
  RFM, календарь;
* `test_quality.py` — правила, метрики, карантин, блокирующие нарушения;
* `test_scd.py` — SCD2 исполняется на локальном Delta: закрытие версии,
  идемпотентность, инвариант «одна текущая версия»;
* `test_datagen.py` — генератор воспроизводим и «управляемо грязен».

## Частые проблемы

**`bundle validate` ругается на путь к ноутбуку** — пути в `resources/jobs/*.yml`
относительны файлу YAML, а не корню репозитория.

**Джоба не видит `ecom`** — в ноутбуке нет `%run ../_bootstrap`.

**`bundle deploy` перезаписал изменения, сделанные в UI** — так и задумано:
источник истины — репозиторий.

**PAT протух** — в Free Edition у токена ограниченный срок; перевыпустите
и обновите GitHub Secret.

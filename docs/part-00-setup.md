# Part 0 — Setup: Unity Catalog, Volumes, данные

**Время:** ~1 час. **Результат:** каталог `ecom_dev` с данными в landing-зоне.

## 1. Проверить, что у вас Free Edition

Откройте **Compute**. Если там только «Serverless» и нет кнопки создания
кластера — это Free Edition. Ограничения, которые важны для проекта:

* только serverless, один SQL warehouse размера 2X-Small;
* один workspace и один metastore;
* максимум 5 одновременных задач джоб;
* один активный пайплайн каждого типа;
* Python и SQL (Scala и R не поддерживаются);
* исходящий интернет ограничен доверенными доменами — поэтому данные
  генерируем локально, а не качаем.

## 2. Подключить GitHub

1. **Settings → Linked accounts → Git integration**: провайдер GitHub,
   личный access token с правом `repo`.
2. **Workspace → Repos → Add repo**: URL вашего репозитория.
3. Проверьте, что в Git folder виден `notebooks/` и `src/`.

Дальше работаете в ветке: `feature/part0-setup` → PR → merge.

## 3. Создать объекты Unity Catalog

Запустите `notebooks/00_setup/00_create_catalog.sql` (виджет `catalog` = `ecom_dev`).

Создаётся:

```
ecom_dev
├── raw     (+ volume landing)
├── bronze
├── silver
├── gold
├── ops     (+ таблица pipeline_audit)
├── ml
└── ldp
```

**Что здесь стоит понять.** В Unity Catalog нет «просто таблицы»: любое имя —
трёхуровневое. Volume — это файловое хранилище внутри каталога, доступное
по обычному пути `/Volumes/ecom_dev/raw/landing/...`; туда можно писать
и Python'ом через `open()`, и Spark'ом. DBFS в новых воркспейсах использовать
не нужно.

## 4. Сгенерировать данные

Запустите `notebooks/00_setup/01_generate_raw_data.py`:

| Виджет | Значение | Смысл |
|---|---|---|
| `catalog` | `ecom_dev` | |
| `start_date` | `2026-08-01` | первый день данных |
| `days` | `21` | сколько дней |
| `orders_per_day` | `800` | ~17k заказов и ~50k позиций суммарно |
| `reset` | `false` | `true` полностью очищает landing |

Получится примерно:

```
orders         21 файл   JSON Lines
order_items    21 файл   JSON Lines
customers      21 файл   CSV (1 полный снимок + 20 дельт)
products        1 файл   JSON Lines
clickstream   ~110 файлов JSON Lines по 800 событий
```

## 5. Проверка

```sql
USE CATALOG ecom_dev;
LIST '/Volumes/ecom_dev/raw/landing/orders';
SELECT count(*) FROM json.`/Volumes/ecom_dev/raw/landing/orders`;
```

## На что обратить внимание

Генератор специально портит данные: ~2% заказов без `customer_id` или с датой
из будущего, ~1% полных дублей, отрицательные количества и цены, 3% событий
приходят «вчерашним числом». Всё это нужно в Part 1 (дедуп), Part 3 (watermark)
и Part 5 (карантин) — не «чините» источник, он такой намеренно.

## Частые проблемы

**`CREATE CATALOG` не проходит** — у пользователя нет прав на metastore.
В Free Edition вы владелец по умолчанию; на корпоративном воркспейсе
попросите `CREATE CATALOG` у администратора или используйте существующий каталог.

**Ноутбук не видит `ecom`** — `%run ../_bootstrap` не выполнен или ноутбук
запущен не из Git folder. Bootstrap ищет `src/ecom` вверх по дереву от
рабочей директории.

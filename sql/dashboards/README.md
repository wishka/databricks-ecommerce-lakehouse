# Дашборд и Genie

AI/BI Dashboard хранится в воркспейсе как объект `.lvdash.json`. Экспортировать
его можно из UI (**⋮ → Export**) и положить сюда — тогда дашборд тоже будет
версионироваться в репозитории.

```
sql/dashboards/
├── README.md
└── ecom_overview.lvdash.json   ← появится после экспорта
```

Чтобы бандл разворачивал дашборд вместе с остальным, добавьте в
`resources/dashboards/ecom_overview.yml`:

```yaml
resources:
  dashboards:
    ecom_overview:
      display_name: "[${bundle.target}] E-commerce overview"
      file_path: ../../sql/dashboards/ecom_overview.lvdash.json
      warehouse_id: ${var.warehouse_id}
```

и переменную `warehouse_id` в `databricks.yml` (id берётся из URL SQL warehouse).

## Состав дашборда

| Виджет | Датасет | Тип |
|---|---|---|
| Выручка по дням | `01_revenue_overview.sql` | line |
| Выручка по категориям | `01_revenue_overview.sql` | bar |
| AOV и доля скидок | `01_revenue_overview.sql` | counter |
| Когортное удержание | `02_cohort_retention.sql` | heatmap |
| Воронка по устройствам | `03_funnel.sql` | bar |
| Риск оттока: топ по LTV | `gold.customer_churn_scores` | table |

Фильтры `date_from`, `date_to`, `channel` привязываются ко всем датасетам сразу.

## Genie space

Таблицы: `gold.daily_sales`, `gold.fct_order_items`, `gold.customer_rfm`,
`gold.dim_customer`, `gold.customer_churn_scores`.

Инструкции спейса (вставить в **Instructions**):

```
Выручка — это net_amount в gold.fct_order_items при is_revenue = true.
Отменённые и возвращённые заказы (status in cancelled, returned) в выручку не входят.
Активный клиент — совершивший покупку за последние 30 дней.
AOV — net_revenue / orders_cnt.
Валюта всегда EUR.
Когда спрашивают про «риск оттока», используй gold.customer_churn_scores.churn_probability.
```

Примеры вопросов добавляйте парами «вопрос → SQL» — это влияет на качество
ответов сильнее всего.

-- Когортный анализ удержания по месяцу первой покупки.
--
-- Классическая задача на оконные функции: у каждого клиента находим месяц
-- первой покупки, затем считаем, в каких последующих месяцах он возвращался.

WITH first_purchase AS (
  SELECT
    customer_id,
    date_trunc('MONTH', min(order_date)) AS cohort_month
  FROM IDENTIFIER(:catalog || '.gold.fct_order_items')
  WHERE is_revenue AND customer_id IS NOT NULL
  GROUP BY customer_id
),
activity AS (
  SELECT DISTINCT
    f.customer_id,
    date_trunc('MONTH', f.order_date) AS active_month
  FROM IDENTIFIER(:catalog || '.gold.fct_order_items') f
  WHERE f.is_revenue AND f.customer_id IS NOT NULL
),
cohorts AS (
  SELECT
    fp.cohort_month,
    months_between(a.active_month, fp.cohort_month)::INT AS month_offset,
    count(DISTINCT a.customer_id)                        AS active_customers
  FROM activity a
  JOIN first_purchase fp USING (customer_id)
  GROUP BY 1, 2
),
sizes AS (
  SELECT cohort_month, count(*) AS cohort_size
  FROM first_purchase
  GROUP BY cohort_month
)
SELECT
  c.cohort_month,
  s.cohort_size,
  c.month_offset,
  c.active_customers,
  round(100.0 * c.active_customers / s.cohort_size, 1) AS retention_pct
FROM cohorts c
JOIN sizes s USING (cohort_month)
ORDER BY c.cohort_month, c.month_offset;

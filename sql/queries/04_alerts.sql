-- Запросы под Databricks Alerts. Каждый возвращает ОДНО число —
-- так проще задать условие срабатывания.

-- =====================================================================
-- Алерт 1: падение выручки. Условие: value < -25
-- Смысл: вчерашняя выручка отклонилась от среднего за предыдущие 7 дней.
-- =====================================================================
WITH daily AS (
  SELECT order_date, sum(net_revenue) AS revenue
  FROM IDENTIFIER(:catalog || '.gold.daily_sales')
  GROUP BY order_date
),
latest AS (SELECT max(order_date) AS d FROM daily)
SELECT round(
  100.0 * (
    (SELECT revenue FROM daily, latest WHERE order_date = d)
    - (SELECT avg(revenue) FROM daily, latest WHERE order_date BETWEEN date_sub(d, 7) AND date_sub(d, 1))
  ) / nullif((SELECT avg(revenue) FROM daily, latest WHERE order_date BETWEEN date_sub(d, 7) AND date_sub(d, 1)), 0),
  1
) AS revenue_change_pct;

-- =====================================================================
-- Алерт 2: деградация качества данных. Условие: value > 0
-- Считает правила severity='fail', провалившиеся в последнем запуске.
-- =====================================================================
-- SELECT count(*) AS blocking_failures
-- FROM (
--   SELECT *, row_number() OVER (PARTITION BY dataset, rule_name ORDER BY run_ts DESC) AS rn
--   FROM IDENTIFIER(:catalog || '.ops.dq_results')
-- )
-- WHERE rn = 1 AND severity = 'fail' AND NOT passed;

-- =====================================================================
-- Алерт 3: конвейер не отработал. Условие: value > 26
-- Часы с момента последней успешной загрузки.
-- =====================================================================
-- SELECT round((unix_timestamp() - unix_timestamp(max(run_ts))) / 3600.0, 1) AS hours_since_last_run
-- FROM IDENTIFIER(:catalog || '.ops.dq_results');

-- =====================================================================
-- Алерт 4: рост доли отменённых заказов. Условие: value > 15
-- =====================================================================
-- SELECT round(100.0 * sum(CASE WHEN NOT is_revenue THEN 1 ELSE 0 END) / count(*), 1) AS cancelled_pct
-- FROM IDENTIFIER(:catalog || '.gold.fct_order_items')
-- WHERE order_date >= current_date() - INTERVAL 7 DAYS;

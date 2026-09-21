-- Датасет дашборда: динамика выручки
-- Параметры дашборда: :catalog, :date_from, :date_to, :channel
--
-- Приём: параметр канала со значением 'all' избавляет от двух версий запроса.

SELECT
  d.order_date,
  d.channel,
  d.category,
  sum(d.net_revenue)                                   AS net_revenue,
  sum(d.orders_cnt)                                    AS orders,
  sum(d.units)                                         AS units,
  round(sum(d.net_revenue) / nullif(sum(d.orders_cnt), 0), 2) AS avg_order_value,
  round(100.0 * sum(d.discount_total) / nullif(sum(d.net_revenue) + sum(d.discount_total), 0), 2)
    AS discount_share_pct
FROM IDENTIFIER(:catalog || '.gold.daily_sales') d
WHERE d.order_date BETWEEN :date_from AND :date_to
  AND (:channel = 'all' OR d.channel = :channel)
GROUP BY d.order_date, d.channel, d.category
ORDER BY d.order_date;

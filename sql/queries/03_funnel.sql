-- Воронка клик-стрима: просмотр → корзина → оформление.
--
-- Считаем по сессиям, а не по событиям: иначе один пользователь,
-- десять раз обновивший страницу, исказит конверсию.

WITH per_session AS (
  SELECT
    session_id,
    device,
    max(CASE WHEN event_type = 'product_view'  THEN 1 ELSE 0 END) AS viewed,
    max(CASE WHEN event_type = 'add_to_cart'   THEN 1 ELSE 0 END) AS carted,
    max(CASE WHEN event_type = 'checkout'      THEN 1 ELSE 0 END) AS checked_out
  FROM IDENTIFIER(:catalog || '.silver.clickstream_events')
  WHERE event_date BETWEEN :date_from AND :date_to
  GROUP BY session_id, device
)
SELECT
  device,
  count(*)                                                     AS sessions,
  sum(viewed)                                                  AS with_product_view,
  sum(carted)                                                  AS with_add_to_cart,
  sum(checked_out)                                             AS with_checkout,
  round(100.0 * sum(carted)      / nullif(sum(viewed), 0), 1)  AS view_to_cart_pct,
  round(100.0 * sum(checked_out) / nullif(sum(carted), 0), 1)  AS cart_to_checkout_pct,
  round(100.0 * sum(checked_out) / count(*), 1)                AS session_conversion_pct
FROM per_session
GROUP BY device
ORDER BY sessions DESC;

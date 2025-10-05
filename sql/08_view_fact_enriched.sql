CREATE OR REPLACE VIEW `gm-sales-analytics.gm_sales.vw_fact_enriched` AS
SELECT
  f.order_id,
  DATE(f.order_date) AS order_date,
  DATE(f.ship_date) AS ship_date,
  f.product_name,
  p.product_id,
  f.segment,
  f.country,
  f.state,
  f.city,
  c.customer_id,
  UNIX_DATE(DATE(f.order_date)) AS date_id,
  f.category,
  f.sub_category,
  f.quantity,
  f.discount,
  f.sales,
  f.profit
FROM `gm-sales-analytics.gm_sales.fact_sales` AS f
LEFT JOIN `gm-sales-analytics.gm_sales.dim_products` AS p
  ON f.product_name = p.product_name
LEFT JOIN `gm-sales-analytics.gm_sales.dim_customers` AS c
  ON f.segment = c.segment
  AND f.country = c.country
  AND f.state = c.state
  AND f.city = c.city;

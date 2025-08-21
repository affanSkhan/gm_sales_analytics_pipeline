CREATE OR REPLACE TABLE `gm-sales-analytics.gm_sales.mart_top_products` AS
SELECT
  product_name,
  SUM(sales) AS total_sales
FROM `gm-sales-analytics.gm_sales.fact_sales`
GROUP BY product_name
ORDER BY total_sales DESC
LIMIT 20;
CREATE OR REPLACE TABLE `gm-sales-analytics.gm_sales.dim_products` AS
SELECT
  FARM_FINGERPRINT(product_name) AS product_id,   -- stable int64 key
  product_name,
  category,
  sub_category
FROM (
  SELECT DISTINCT product_name, category, sub_category
  FROM `gm-sales-analytics.gm_sales.fact_sales`
);

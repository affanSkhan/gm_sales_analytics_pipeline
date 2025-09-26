CREATE OR REPLACE TABLE `gm-sales-analytics.gm_sales.dim_customers` AS
SELECT
  FARM_FINGERPRINT(CONCAT(COALESCE(segment,''), '|', COALESCE(country,''), '|', COALESCE(state,''), '|', COALESCE(city,''))) AS customer_id,
  segment,
  country,
  state,
  city
FROM (
  SELECT DISTINCT segment, country, state, city
  FROM `gm-sales-analytics.gm_sales.fact_sales`
);

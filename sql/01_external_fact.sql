-- Create External Table
CREATE OR REPLACE EXTERNAL TABLE `gm-sales-analytics.gm_sales.ext_sales`
OPTIONS (
  format = 'CSV',
  uris = ['gs://gm-sales-raw-data-<your-uniq>/inbound/sales/*.csv'],
  skip_leading_rows = 1
);

-- Create Partitioned Fact Table
CREATE OR REPLACE TABLE `gm-sales-analytics.gm_sales.fact_sales`
PARTITION BY order_date
AS
SELECT
  SAFE_CAST(order_id AS STRING) AS order_id,
  DATE(order_date) AS order_date,
  DATE(ship_date) AS ship_date,
  INITCAP(segment) AS segment,
  country, state, city,
  category, sub_category, product_name,
  SAFE_CAST(quantity AS INT64) AS quantity,
  SAFE_CAST(discount AS NUMERIC) AS discount,
  SAFE_CAST(sales AS NUMERIC) AS sales,
  SAFE_CAST(profit AS NUMERIC) AS profit
FROM `gm-sales-analytics.gm_sales.ext_sales`
WHERE order_id IS NOT NULL;

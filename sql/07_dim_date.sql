CREATE OR REPLACE TABLE `gm-sales-analytics.gm_sales.dim_date` AS
SELECT
  UNIX_DATE(DATE(order_date)) AS date_id,    -- int key
  DATE(order_date) AS calendar_date,
  EXTRACT(YEAR FROM DATE(order_date)) AS year,
  EXTRACT(MONTH FROM DATE(order_date)) AS month,
  EXTRACT(DAY FROM DATE(order_date)) AS day,
  FORMAT_DATE('%A', DATE(order_date)) AS weekday,
  FORMAT_DATE('%B', DATE(order_date)) AS month_name,
  DATE_TRUNC(DATE(order_date), WEEK(MONDAY)) AS week_start
FROM (
  SELECT DISTINCT DATE(order_date) AS order_date
  FROM `gm-sales-analytics.gm_sales.fact_sales`
)
ORDER BY calendar_date;

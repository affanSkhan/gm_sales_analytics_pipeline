CREATE OR REPLACE TABLE `gm-sales-analytics.gm_sales.mart_daily_sales` AS
SELECT
  order_date,
  SUM(sales)  AS total_sales,
  SUM(profit) AS total_profit
FROM `gm-sales-analytics.gm_sales.fact_sales`
GROUP BY order_date;
-- sql/02_analytics_queries.sql
-- Example analytics queries for Sales Analytics project
-- Project: gm-sales-analytics, Dataset: gm_sales
-- Uses the enriched view: `gm-sales-analytics.gm_sales.vw_fact_enriched`

/* -------------------------

1. Profit by Category
   Description: total & average profit and sales per category.
   Remove or modify the WHERE clause to change time window.
   ------------------------- */
   -- Overall (no date filter)
   SELECT
   category,
   SUM(SAFE_CAST(profit AS NUMERIC)) AS total_profit,
   ROUND(AVG(SAFE_CAST(profit AS NUMERIC)), 2) AS avg_profit,
   SUM(SAFE_CAST(sales AS NUMERIC)) AS total_sales,
   COUNT(1) AS orders_count
   FROM `gm-sales-analytics.gm_sales.vw_fact_enriched`
   GROUP BY category
   ORDER BY total_profit DESC;

-- Example: Profit by category for a specific year (uncomment to use)
-- SELECT
--   category,
--   SUM(SAFE_CAST(profit AS NUMERIC)) AS total_profit,
--   SUM(SAFE_CAST(sales AS NUMERIC)) AS total_sales,
--   COUNT(1) AS orders_count
-- FROM `gm-sales-analytics.gm_sales.vw_fact_enriched`
-- WHERE order_date BETWEEN DATE('2019-01-01') AND DATE('2019-12-31')
-- GROUP BY category
-- ORDER BY total_profit DESC;

/* -------------------------
2) Sales by Region
Description: total sales and profit aggregated by country and state.
------------------------- */
SELECT
COALESCE(country, 'UNKNOWN') AS country,
COALESCE(state, 'UNKNOWN') AS state,
SUM(SAFE_CAST(sales AS NUMERIC)) AS total_sales,
SUM(SAFE_CAST(profit AS NUMERIC)) AS total_profit,
COUNT(1) AS orders_count
FROM `gm-sales-analytics.gm_sales.vw_fact_enriched`
GROUP BY country, state
ORDER BY total_sales DESC
LIMIT 100;

/* -------------------------
3) Top 10 Products by Sales
Description: the top 10 products by total sales revenue.
------------------------- */
SELECT
product_name,
product_id,
SUM(SAFE_CAST(sales AS NUMERIC)) AS total_sales,
SUM(SAFE_CAST(quantity AS INT64)) AS total_quantity,
COUNT(1) AS orders_count
FROM `gm-sales-analytics.gm_sales.vw_fact_enriched`
GROUP BY product_name, product_id
ORDER BY total_sales DESC
LIMIT 10;

-- Optional additional useful query: Monthly sales trend
-- SELECT
--   EXTRACT(YEAR FROM order_date) AS year,
--   EXTRACT(MONTH FROM order_date) AS month,
--   SUM(SAFE_CAST(sales AS NUMERIC)) AS total_sales,
--   SUM(SAFE_CAST(profit AS NUMERIC)) AS total_profit
-- FROM `gm-sales-analytics.gm_sales.vw_fact_enriched`
-- GROUP BY year, month
-- ORDER BY year, month;

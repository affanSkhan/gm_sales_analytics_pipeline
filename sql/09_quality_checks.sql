-- ============================================================
-- 06_quality_checks.sql
-- Data quality checks: missing order_id, negative sales/profit
-- Inserts results into gm_sales.quality_logs with sample rows (up to 10)
-- ============================================================

-- 0. (Optional) See if quality_logs table already exists
SELECT table_name
FROM `gm-sales-analytics`.gm_sales.INFORMATION_SCHEMA.TABLES
WHERE table_name = 'quality_logs';

-- 1. Create the quality_logs table (runs history)
-- If your console errors on IF NOT EXISTS, remove that clause.
CREATE TABLE IF NOT EXISTS `gm-sales-analytics.gm_sales.quality_logs` (
  check_timestamp TIMESTAMP,
  check_name STRING,
  status STRING,             -- PASS / FAIL
  records_affected INT64,
  sample_values STRING,      -- JSON-like samples separated by " | "
  details STRING
);

-- 2. Check: Missing order_id (NULL or empty string)
INSERT INTO `gm-sales-analytics.gm_sales.quality_logs`
(check_timestamp, check_name, status, records_affected, sample_values, details)
SELECT
  CURRENT_TIMESTAMP() AS check_timestamp,
  'Missing order_id' AS check_name,
  IF(cnt = 0, 'PASS', 'FAIL') AS status,
  cnt AS records_affected,
  IF(cnt = 0, '',
     ARRAY_TO_STRING(
       ARRAY(
         SELECT TO_JSON_STRING(t)
         FROM (
           SELECT AS STRUCT order_id, order_date, product_name, sales
           FROM `gm-sales-analytics.gm_sales.fact_sales`
           WHERE order_id IS NULL OR TRIM(CAST(order_id AS STRING)) = ''
           LIMIT 10
         ) t
       ), ' | '
     )
  ) AS sample_values,
  'Rows where order_id is NULL or empty' AS details
FROM (
  SELECT COUNT(*) AS cnt
  FROM `gm-sales-analytics.gm_sales.fact_sales`
  WHERE order_id IS NULL OR TRIM(CAST(order_id AS STRING)) = ''
);

-- 3. Check: Negative sales or profit (values < 0)
INSERT INTO `gm-sales-analytics.gm_sales.quality_logs`
(check_timestamp, check_name, status, records_affected, sample_values, details)
SELECT
  CURRENT_TIMESTAMP() AS check_timestamp,
  'Negative sales or profit' AS check_name,
  IF(cnt = 0, 'PASS', 'FAIL') AS status,
  cnt AS records_affected,
  IF(cnt = 0, '',
     ARRAY_TO_STRING(
       ARRAY(
         SELECT TO_JSON_STRING(t)
         FROM (
           SELECT AS STRUCT order_id, product_name, sales, profit
           FROM `gm-sales-analytics.gm_sales.fact_sales`
           WHERE SAFE_CAST(sales AS NUMERIC) < 0 OR SAFE_CAST(profit AS NUMERIC) < 0
           LIMIT 10
         ) t
       ), ' | '
     )
  ) AS sample_values,
  'Rows where sales < 0 or profit < 0' AS details
FROM (
  SELECT COUNT(*) AS cnt
  FROM `gm-sales-analytics.gm_sales.fact_sales`
  WHERE SAFE_CAST(sales AS NUMERIC) < 0 OR SAFE_CAST(profit AS NUMERIC) < 0
);

-- 4. (Optional) Quick check output - show latest log entries (for convenience)
SELECT *
FROM `gm-sales-analytics.gm_sales.quality_logs`
ORDER BY check_timestamp DESC
LIMIT 20;

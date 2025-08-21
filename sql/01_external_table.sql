-- Create External Table
CREATE OR REPLACE EXTERNAL TABLE `gm-sales-analytics.gm_sales.ext_sales`
OPTIONS (
  format = 'CSV',
  uris = ['gs://gm-sales-raw-data-<your-uniq>/inbound/sales/*.csv'],
  skip_leading_rows = 1
);

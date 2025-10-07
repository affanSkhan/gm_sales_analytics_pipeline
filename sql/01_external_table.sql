CREATE OR REPLACE EXTERNAL TABLE `gm-sales-analytics.gm_sales.ext_sales`
OPTIONS (
  format = 'CSV',
  uris = ['gs://asia-south1-sales-etl-compo-bf620c4f-bucket/data/sales_data.csv'],
  skip_leading_rows = 1,
  field_delimiter = ','
);

# Sales Analytics Data Pipeline (GCP)

**Stack:** GCS, BigQuery (External + Partitioned tables), Cloud Composer (Airflow) (planned), Looker Studio (planned)

## Day 1: Data Landing + Modeling

* Created GCP project and enabled required APIs.
* Created GCS bucket `gs://gm-sales-raw-data-affan` and uploaded `sales_small.csv` to `inbound/sales/`.
* Built BigQuery external table (`ext_sales`) to read CSVs directly from GCS.
* Built partitioned `fact_sales` table (partitioned by `order_date`) with type casting and basic cleaning.
* Verified with sample queries.

## Dataset

Columns: `order_id`, `order_date`, `ship_date`, `segment`, `country`, `state`, `city`, `category`, `sub_category`, `product_name`, `quantity`, `discount`, `sales`, `profit`

## Week 2: Data Warehouse Modeling & Quality (Day 8–14)

**Status:** Completed

### What we implemented

* **Dimension tables**

  * `dim_products` — distinct products with `product_id` (FARM_FINGERPRINT(product_name)), `product_name`, `category`, `sub_category`.
  * `dim_customers` — distinct (segment, country, state, city) combinations with `customer_id` (fingerprint).
  * `dim_date` — calendar/date dimension (date_id = UNIX_DATE, year, month, day, weekday, month_name, week_start).
* **Enriched analytical view**

  * `vw_fact_enriched`: a view that joins `fact_sales` to the dimension tables for convenient querying and dashboards. (View keeps storage minimal while providing joined data.)
* **Data quality checks**

  * Implemented `sql/06_quality_checks.sql` which:

    * Creates `gm_sales.quality_logs` (check_timestamp, check_name, status, records_affected, sample_values, details).
    * Runs checks for **missing `order_id`** and **negative `sales` or `profit`**, inserting results and up to 10 sample failing rows.
* **Analytics queries**

  * Implemented `sql/02_analytics_queries.sql` including:

    * Profit by category.
    * Sales by region (country, state).
    * Top 10 products by sales.
* **Documentation & diagrams**

  * Added ER diagram (star schema) to `docs/er_diagram.png` (generated with dbdiagram.io / diagrams.net).
  * Saved screenshots of sample queries and table listings in `docs/`.

### Files added / updated

* `sql/02_analytics_queries.sql` — analytics example queries.
* `sql/06_quality_checks.sql` — data quality checks and `quality_logs` table.
* `sql/02_dim_products.sql`, `sql/03_dim_customers.sql`, `sql/04_dim_date.sql` — dim creation scripts (if used).
* `sql/05_view_fact_enriched.sql` — enriched view creation.
* `docs/er_diagram.png` — star schema ER diagram.
* `README.md` — this file (updated).

### How to run the checks & queries

* Quality checks: run `sql/06_quality_checks.sql` in BigQuery Compose to populate `gm_sales.quality_logs`.
* Analytics queries: open `sql/02_analytics_queries.sql`, copy any query to BigQuery Compose and run (use date filters to limit bytes scanned).
* View: `SELECT * FROM \`gm-sales-analytics.gm_sales.vw_fact_enriched` LIMIT 10;`

## Next (Week 3 plan)

* Automate the pipeline with Cloud Composer (Airflow) DAG that triggers on new GCS files → refresh external table → load dims/fact → run quality checks.
* Build Looker Studio dashboard using `vw_fact_enriched` as the data source (sales trends, profit by category, region map, top products).
* Finalize report and presentation assets.

---

**Notes on cost control**

* Keep CSV small for development and testing.
* Use views where possible to avoid storing duplicate data.
* Run Composer for demos only; delete the environment after the demo to avoid ongoing charges.
* Use BigQuery dry runs and date filters to estimate and minimize bytes scanned.

If you need, I can also produce a one-page architecture diagram image (PNG/SVG) that you can add to `docs/` — but per your request I did not generate the ER image myself; I provided the steps so you can create and control the diagram.

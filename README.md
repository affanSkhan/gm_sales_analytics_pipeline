# \# Sales Analytics Data Pipeline (GCP)

# 

# \*\*Stack:\*\* GCS, BigQuery (External + Partitioned tables), (Cloud Composer/Airflow soon), Looker Studio (soon)

# 

# \## Day 1: Data Landing + Modeling

# \- Created GCP project and enabled APIs.

# \- Created GCS bucket and uploaded CSV to `inbound/sales/`.

# \- Built BigQuery external table over GCS CSV.

# \- Built partitioned `fact\_sales` table (by order\_date).

# \- Verified with sample queries.

# 

# \## Dataset

# Columns: order\_id, order\_date, ship\_date, segment, country, state, city, category, sub\_category, product\_name, quantity, discount, sales, profit

# 

# \## Next

# \- Day 2: refine SQL + marts

# \- Day 3: Cloud Composer DAG (ETL)

# \- Day 4: Looker Studio dashboard

# \- Day 5: CI/CD + polish

# 


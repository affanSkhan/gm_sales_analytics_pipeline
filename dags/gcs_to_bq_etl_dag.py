# dags/gcs_to_bq_etl_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ID = "gm-sales-analytics"
DATASET = "gm_sales"
SQL_DIR = "/home/airflow/gcs/dags/sql"   # path where your SQL files will live in Composer

default_args = {
    "owner": "affan",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": datetime(2025, 10, 1),
}

with DAG(
    dag_id="gcs_to_bq_etl",
    default_args=default_args,
    schedule_interval=None,   # Manual run; change to cron if you want automation
    catchup=False,
    max_active_runs=1,
    tags=["gm_sales", "etl"],
) as dag:
    
    create_ext_sales = BashOperator(
        task_id="create_ext_sales",
        bash_command=(
            "set -euo pipefail; "
            "echo 'Creating external table ext_sales...' ; "
            f"bq query --use_legacy_sql=false --project_id={PROJECT_ID} < {SQL_DIR}/01_external_table.sql"
        ),
    )


    create_fact = BashOperator(
        task_id="create_fact_sales",
        bash_command=(
            "set -euo pipefail; "
            "echo 'Running create fact_sales...' ; "
            f"bq --quiet query --use_legacy_sql=false --project_id='{PROJECT_ID}' < {SQL_DIR}/02_fact_sales.sql"
        ),
    )

    create_dim_products = BashOperator(
        task_id="create_dim_products",
        bash_command=(
            "set -euo pipefail; " 
            "echo 'Running create dim_products...' ; " 
            f"bq --quiet query --use_legacy_sql=false --project_id='{PROJECT_ID}' < {SQL_DIR}/05_dim_products.sql"
        ),
    )

    create_dim_customers = BashOperator(
        task_id="create_dim_customers",
        bash_command=(
            "set -euo pipefail; " 
            "echo 'Running create dim_customers...' ; " 
            f"bq --quiet query --use_legacy_sql=false --project_id='{PROJECT_ID}' < {SQL_DIR}/06_dim_customers.sql"
        ),
    )

    create_dim_date = BashOperator(
        task_id="create_dim_date",
        bash_command=(
            "set -euo pipefail; " 
            "echo 'Running create dim_date...' ; " 
            f"bq --quiet query --use_legacy_sql=false --project_id='{PROJECT_ID}' < {SQL_DIR}/07_dim_date.sql"
        ),
    )

    run_quality_checks = BashOperator(
        task_id="run_quality_checks",
        bash_command=(
            "set -euo pipefail; " 
            "echo 'Running quality checks...' ; " 
            f"bq --quiet query --use_legacy_sql=false --project_id='{PROJECT_ID}' < {SQL_DIR}/09_quality_checks.sql"
        ),
    )

    # Define dependency flow
    create_ext_sales >> create_fact
    create_fact >> [create_dim_products, create_dim_customers, create_dim_date]
    [create_dim_products, create_dim_customers, create_dim_date] >> run_quality_checks
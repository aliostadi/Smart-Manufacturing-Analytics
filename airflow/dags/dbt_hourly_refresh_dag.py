from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# -------------------------------
# DAG configuration
# -------------------------------
default_args = {
    'owner': 'Aliostadi',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dbt_hourly_refresh',
    default_args=default_args,
    description='Run dbt models every hour automatically',
    schedule_interval='0 * * * *',  # every hour at minute 0
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['dbt', 'etl', 'hourly']
) as dag:

    # -------------------------------
    # 1️⃣ Run dbt inside Docker Compose
    # -------------------------------
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command=('cd /opt/airflow/dbt_project && dbt clean && dbt run --full-refresh'
        )
    )

    run_dbt

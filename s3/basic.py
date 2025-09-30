"""
Airflow 2 DAG - Execute dbt model (Simplest Version)
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ===== UPDATE THESE =====
DBT_PROJECT_DIR = '/opt/airflow/dbt/dbt-mq-ctfrs'
DBT_MODEL = 'fraud_feature_aggregation'
# ========================

with DAG(
    dag_id='dbt_fraud_features',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'fraud'],
) as dag:

    # Run your dbt model
    run_dbt = BashOperator(
        task_id='run_dbt_model',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt run --models {DBT_MODEL}',
    )

    # Test your dbt model
    test_dbt = BashOperator(
        task_id='test_dbt_model',
        bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --models {DBT_MODEL}',
    )

    run_dbt >> test_dbt

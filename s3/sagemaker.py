"""
Airflow 2 DAG - Execute dbt using SageMaker Processing
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerProcessingOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ===== CONFIGURATION =====
AWS_CONN_ID = 'aws_default'
ROLE_ARN = 'arn:aws:iam::YOUR_ACCOUNT_ID:role/SageMakerExecutionRole'
S3_BUCKET = 'your-dbt-bucket'
S3_DBT_CODE = f's3://{S3_BUCKET}/dbt/dbt-mq-ctfrs/'
S3_OUTPUT = f's3://{S3_BUCKET}/dbt-output/'
REGION = 'us-east-1'
DBT_MODEL = 'fraud_feature_aggregation'
ECR_IMAGE = 'YOUR_ACCOUNT_ID.dkr.ecr.us-east-1.amazonaws.com/dbt-processor:latest'
# =========================

# SageMaker Processing Job Configuration
processing_config = {
    'ProcessingJobName': f'dbt-fraud-{{{{ ts_nodash }}}}',
    'ProcessingInputs': [
        {
            'InputName': 'dbt-code',
            'S3Input': {
                'S3Uri': S3_DBT_CODE,
                'LocalPath': '/opt/ml/processing/input/dbt',
                'S3DataType': 'S3Prefix',
                'S3InputMode': 'File',
            }
        }
    ],
    'ProcessingOutputConfig': {
        'Outputs': [
            {
                'OutputName': 'dbt-output',
                'S3Output': {
                    'S3Uri': S3_OUTPUT,
                    'LocalPath': '/opt/ml/processing/output',
                    'S3UploadMode': 'EndOfJob'
                }
            }
        ]
    },
    'ProcessingResources': {
        'ClusterConfig': {
            'InstanceCount': 1,
            'InstanceType': 'ml.m5.xlarge',
            'VolumeSizeInGB': 30
        }
    },
    'StoppingCondition': {
        'MaxRuntimeInSeconds': 3600
    },
    'AppSpecification': {
        'ImageUri': ECR_IMAGE,
        'ContainerEntrypoint': ['bash', '-c'],
        'ContainerArguments': [
            f'cd /opt/ml/processing/input/dbt && dbt run --models {DBT_MODEL} && dbt test --models {DBT_MODEL}'
        ]
    },
    'RoleArn': ROLE_ARN
}

with DAG(
    dag_id='dbt_fraud_sagemaker',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'sagemaker', 'fraud'],
) as dag:

    # Run dbt using SageMaker
    run_dbt_sagemaker = SageMakerProcessingOperator(
        task_id='run_dbt_processing',
        config=processing_config,
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        check_interval=30,
    )

    run_dbt_sagemaker

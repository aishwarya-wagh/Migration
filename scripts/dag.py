from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.sagemaker.operators.sagemaker import SageMakerProcessingOperator
from airflow.providers.snowflake.transfers.snowflake_to_s3 import SnowflakeToS3Operator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'retries': 1
}

with DAG(
    's3_export_pipeline',
    default_args=DEFAULT_ARGS,
    schedule_interval='@daily',
    catchup=False
) as dag:

    # 1. Run dbt model (assuming dbt run is orchestrated separately)
    # Alternatively use BashOperator to run "dbt run --models last_3_days"
    
    # 2. Export data from Snowflake to S3
    export_to_s3 = SnowflakeToS3Operator(
        task_id='export_to_s3',
        snowflake_conn_id='snowflake_conn',
        s3_conn_id='aws_conn',
        sql='SELECT * FROM {{ ref("last_3_days") }}',
        s3_bucket='your-s3-bucket',
        s3_key='data/last_3_days/',
        file_format='(TYPE = PARQUET)',
        stage='YOUR_SNOWFLAKE_STAGE',  # Pre-defined Snowflake stage
        export_format='PARQUET'
    )

    # 3. SageMaker Processing for lineage tracking
    process_with_lineage = SageMakerProcessingOperator(
        task_id='track_lineage',
        config={
            "ProcessingJobName": "lineage-tracker-{{ ds_nodash }}",
            "ProcessingResources": {
                "ClusterConfig": {
                    "InstanceType": "ml.t3.medium",
                    "InstanceCount": 1,
                    "VolumeSizeInGB": 10
                }
            },
            "ProcessingInputs": [{
                "InputName": "input-data",
                "S3Input": {
                    "S3Uri": "s3://your-s3-bucket/data/last_3_days/",
                    "LocalPath": "/opt/ml/processing/input",
                    "S3DataType": "S3Prefix",
                    "S3InputMode": "File"
                }
            }],
            "ProcessingOutputs": [{
                "OutputName": "lineage-output",
                "S3Output": {
                    "S3Uri": "s3://your-s3-bucket/lineage/",
                    "LocalPath": "/opt/ml/processing/output",
                    "S3UploadMode": "EndOfJob"
                }
            }],
            "AppSpecification": {
                "ImageUri": "sagemaker-processing-container:latest"  # Use appropriate image
            },
            "RoleArn": "arn:aws:iam::123456789012:role/SageMakerRole"
        }
    )

    export_to_s3 >> process_with_lineage

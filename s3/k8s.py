"""
Airflow 2 DAG - DBT with DbtDagParser and KubernetesPodOperator
Working example for fraud_feature_aggregation model
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# Default arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ===== CONFIGURATION =====
SERVICE_NAME = "fraud_score_features"
OWNER = "data-team"
DBT_PROJECT_PATH = "/dbt/dbt-mq-ctfrs"
DATABASE = "your_database"
ROLE = "SVC_FRAUD_SCORE_RL"  # Your database role
NAMESPACE = "airflow"  # K8s namespace
DBT_IMAGE = "your-docker-registry/dbt-image:latest"  # Your dbt Docker image
DBT_TARGET = "prod"  # dev or prod
DBT_MODEL = "fraud_feature_aggregation"

# Volume configuration for dbt project
volume = k8s.V1Volume(
    name='dbt-project',
    config_map=k8s.V1ConfigMapVolumeSource(
        name='dbt-project-config'  # ConfigMap with your dbt project
    )
)

volume_mount = k8s.V1VolumeMount(
    name='dbt-project',
    mount_path='/dbt',
    read_only=True
)

# Secret for database credentials
env_vars = [
    k8s.V1EnvVar(
        name='DB_HOST',
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name='db-credentials',
                key='host'
            )
        )
    ),
    k8s.V1EnvVar(
        name='DB_USER',
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name='db-credentials',
                key='username'
            )
        )
    ),
    k8s.V1EnvVar(
        name='DB_PASSWORD',
        value_from=k8s.V1EnvVarSource(
            secret_key_ref=k8s.V1SecretKeySelector(
                name='db-credentials',
                key='password'
            )
        )
    ),
    k8s.V1EnvVar(name='DB_NAME', value=DATABASE),
    k8s.V1EnvVar(name='DB_SCHEMA', value='analytics'),
    k8s.V1EnvVar(name='DB_ROLE', value=ROLE),
]

# ===================================================================
# DAG Definition
# ===================================================================

with DAG(
    dag_id=f'{SERVICE_NAME}_dbt_k8s',
    default_args=default_args,
    description='Run dbt models using Kubernetes',
    schedule_interval='@daily',
    catchup=False,
    tags=['dbt', 'k8s', 'fraud'],
) as dag:

    # Task 1: DBT Debug (check connections)
    dbt_debug = KubernetesPodOperator(
        task_id='dbt_debug',
        name='dbt-debug',
        namespace=NAMESPACE,
        image=DBT_IMAGE,
        cmds=['dbt'],
        arguments=[
            'debug',
            '--project-dir', DBT_PROJECT_PATH,
            '--profiles-dir', DBT_PROJECT_PATH,
            '--target', DBT_TARGET
        ],
        env_vars=env_vars,
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
        log_events_on_failure=True,
    )

    # Task 2: DBT Seed (if you have seed files)
    dbt_seed = KubernetesPodOperator(
        task_id='dbt_seed',
        name='dbt-seed',
        namespace=NAMESPACE,
        image=DBT_IMAGE,
        cmds=['dbt'],
        arguments=[
            'seed',
            '--project-dir', DBT_PROJECT_PATH,
            '--profiles-dir', DBT_PROJECT_PATH,
            '--target', DBT_TARGET
        ],
        env_vars=env_vars,
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
        log_events_on_failure=True,
    )

    # Task 3: DBT Run - Execute the model
    dbt_run = KubernetesPodOperator(
        task_id='dbt_run_fraud_features',
        name='dbt-run-fraud-features',
        namespace=NAMESPACE,
        image=DBT_IMAGE,
        cmds=['dbt'],
        arguments=[
            'run',
            '--models', DBT_MODEL,
            '--project-dir', DBT_PROJECT_PATH,
            '--profiles-dir', DBT_PROJECT_PATH,
            '--target', DBT_TARGET
        ],
        env_vars=env_vars,
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
        log_events_on_failure=True,
        resources=k8s.V1ResourceRequirements(
            requests={'memory': '512Mi', 'cpu': '500m'},
            limits={'memory': '2Gi', 'cpu': '2000m'}
        ),
    )

    # Task 4: DBT Test - Test the model
    dbt_test = KubernetesPodOperator(
        task_id='dbt_test_fraud_features',
        name='dbt-test-fraud-features',
        namespace=NAMESPACE,
        image=DBT_IMAGE,
        cmds=['dbt'],
        arguments=[
            'test',
            '--models', DBT_MODEL,
            '--project-dir', DBT_PROJECT_PATH,
            '--profiles-dir', DBT_PROJECT_PATH,
            '--target', DBT_TARGET
        ],
        env_vars=env_vars,
        volumes=[volume],
        volume_mounts=[volume_mount],
        is_delete_operator_pod=True,
        get_logs=True,
        log_events_on_failure=True,
    )

    # Task dependencies
    dbt_debug >> dbt_seed >> dbt_run >> dbt_test


# ===================================================================
# Alternative: Using DbtDagParser (Cosmos)
# ===================================================================

"""
If you're using astronomer-cosmos (DbtDagParser), here's the approach:

First install: pip install astronomer-cosmos

Then use this pattern:
"""

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping

# Profile configuration
profile_config = ProfileConfig(
    profile_name='dbt_mq_ctfrs',
    target_name=DBT_TARGET,
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id='postgres_default',  # Airflow connection ID
        profile_args={
            'schema': 'analytics',
        },
    ),
)

# Project configuration
project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

# Execution configuration
execution_config = ExecutionConfig(
    dbt_executable_path='/usr/local/bin/dbt',
)

# Create DAG using Cosmos DbtDag
dbt_cosmos_dag = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    operator_args={
        'install_deps': True,  # Run dbt deps
        'full_refresh': False,
    },
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id='fraud_features_cosmos',
    default_args=default_args,
    tags=['dbt', 'cosmos', 'fraud'],
)


# ===================================================================
# Alternative: DbtDagParser with K8s Executor
# ===================================================================

"""
If using DbtDagParser from cosmos with K8s execution:
"""

from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig, RenderConfig
from cosmos.constants import ExecutionMode

# Execution config for K8s
k8s_execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.KUBERNETES,
)

# Render config for selective model execution
render_config = RenderConfig(
    select=['fraud_feature_aggregation'],  # Only this model
    exclude=[],
)

# Create DAG with K8s execution
dbt_k8s_cosmos_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_PROJECT_PATH,
    ),
    profile_config=ProfileConfig(
        profile_name='dbt_mq_ctfrs',
        target_name=DBT_TARGET,
        profile_mapping=PostgresUserPasswordProfileMapping(
            conn_id='postgres_default',
            profile_args={'schema': 'analytics'},
        ),
    ),
    execution_config=k8s_execution_config,
    render_config=render_config,
    operator_args={
        'image': DBT_IMAGE,
        'namespace': NAMESPACE,
        'env_vars': env_vars,
        'is_delete_operator_pod': True,
        'get_logs': True,
    },
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    dag_id='fraud_features_k8s_cosmos',
    default_args=default_args,
    tags=['dbt', 'k8s', 'cosmos', 'fraud'],
)

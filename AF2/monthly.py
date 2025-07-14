from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
import dependencies as d

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 1, 1),
    "email_on_failure": False,
    "retries": 1,
    "on_failure_callback": d.tagged_slack_failure_callback(
        settings={"TAGS": [
            "QU01UC354UM8",  # @ttalkington
            "QU033ZTX0BDC",  # @awong
            "QU046AUCPYFJ",  # @aserov
        ]},
        pagerduty_team="recon",
        pagerduty_severity="error",
    ),
}

# Calculate date parameters
today = datetime.now(timezone.utc).date()
last_day_of_prev_month = today.replace(day=1) - timedelta(days=1)

# DAG definition
with DAG(
    "pin_transactions_monthly",
    default_args=default_args,
    description="Monthly pin transactions report",
    schedule_interval="0 15 2 * *",  # 2nd day of month @ 3:00 PM UTC
    max_active_runs=1,
    catchup=False,
    tags=["billing", "monthly", "pin", "transactions"],
) as dag:

    # Latest only operator
    latest_only = LatestOnlyOperator(
        task_id="latest-only",
        dag=dag
    )

    # Previous task reference
    previous_task = latest_only

    # Reports configuration
    reports_config = [
        ("interlink_detail", "interlink_detail_monthly_pin_snapshot"),
        ("interlink_summary", "interlink_summary_monthly_pin_trans"),
        ("maestro_detail", "maestro_detail_monthly_pin_snapshot"),
        ("maestro_summary", "maestro_summary_monthly_pin_trans"),
        ("pulse_detail", "pulse_detail_monthly_pin_snapshot"),
        ("pulse_summary", "pulse_summary_monthly_pin_trans"),
    ]

    # Create tasks for each report
    for report_name, report_type in reports_config:
        task = KubernetesPodOperator(
            task_id=f"pin_transactions_monthly_{report_name}",
            name=f"pin-transactions-monthly-{report_name}",
            namespace="default",
            image="your-image-registry/di_report_automation:latest",  # Update with actual image
            resources={
                "request_memory": "512Mi",
                "limit_memory": "1Gi"
            },
            get_logs=True,
            arguments=[
                "-r",
                report_type,
                "-t",
                report_name,
                "-y",
                f"{{{{ dag_run.conf['report_year'] if dag_run.conf and 'report_year' in dag_run.conf else '{last_day_of_prev_month.year}' }}}}",
                "-m",
                f"{{{{ dag_run.conf['report_month'] if dag_run.conf and 'report_month' in dag_run.conf else '{last_day_of_prev_month.month}' }}}}",
                "-l",
                "INFO",
            ],
            env_vars={"ENV": "PROD"},
            dag=dag,
        )
        
        # Set task dependencies
        previous_task >> task
        previous_task = task

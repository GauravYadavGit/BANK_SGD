from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineer",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

CLUSTER_ID = "j-XXXXXXXX"

with DAG(
    dag_id="transaction_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=True,
) as dag:

    # -------------------------------
    # 1. Wait for RAW CSV
    # -------------------------------
    wait_for_raw = S3KeySensor(
        task_id="wait_for_raw_csv",
        bucket_name="data-lake",
        bucket_key="raw/transactions/date={{ ds }}/_SUCCESS",
        poke_interval=300,
        timeout=3600,
    )

    # -------------------------------
    # 2. CSV → PARQUET (BRONZE)
    # -------------------------------
    csv_to_parquet = EmrAddStepsOperator(
        task_id="csv_to_parquet",
        job_flow_id=CLUSTER_ID,
        steps=[
            {
                "Name": "CSV to Parquet",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "s3://your-bucket/source_to_parquet.py",
                        "{{ ds }}",
                    ],
                },
            }
        ],
    )

    watch_csv = EmrStepSensor(
        task_id="watch_csv_step",
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='csv_to_parquet', key='return_value')[0] }}",
    )

    # -------------------------------
    # 3. Wait for EXCHANGE
    # -------------------------------
    wait_for_exchange = S3KeySensor(
        task_id="wait_for_exchange",
        bucket_name="data-lake",
        bucket_key="exchange_rates/date={{ ds }}/_SUCCESS",
        poke_interval=300,
        timeout=3600,
    )

    # -------------------------------
    # 4. Transformation Job
    # -------------------------------
    run_transform = EmrAddStepsOperator(
        task_id="run_transformation",
        job_flow_id=CLUSTER_ID,
        steps=[
            {
                "Name": "Transaction Transformation",
                "ActionOnFailure": "CANCEL_AND_WAIT",
                "HadoopJarStep": {
                    "Jar": "command-runner.jar",
                    "Args": [
                        "spark-submit",
                        "s3://your-bucket/main.py",
                        "{{ ds }}",
                    ],
                },
            }
        ],
    )

    watch_transform = EmrStepSensor(
        task_id="watch_transform",
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='run_transformation', key='return_value')[0] }}",
    )

    # -------------------------------
    # FLOW
    # -------------------------------
    wait_for_raw >> csv_to_parquet >> watch_csv
    watch_csv >> wait_for_exchange
    wait_for_exchange >> run_transform >> watch_transform

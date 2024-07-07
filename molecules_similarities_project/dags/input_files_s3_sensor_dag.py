from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta



bucket_name = Variable.get("aws_s3_bucket_name")
file_prefix = Variable.get("aws_s3_prefix")


default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
        dag_id="s3_files_sensor_dag",
        start_date=datetime(
            2023,
            7,
            1
        ),
        tags=[
            "s3_bucket",
            "sensor",
            "molecules_similarities_project"
        ],
        default_args=default_args,
        description="A DAG with S3KeySensor that checks whether new files are \
                 presented in s3 bucket and triggers another DAG.",
        schedule_interval="@monthly",
        catchup=False,
) as dag:
    start_op = EmptyOperator(
        task_id="start_check_for_files"
    )

    check_s3_bucket_op = S3KeySensor(
        task_id="s3_key_sensor",
        bucket_name=bucket_name,
        bucket_key=file_prefix + "data_*.csv",
        wildcard_match=True,
        aws_conn_id="aws_s3_bucket",
        timeout=60 * 5,
        poke_interval=60,
        mode="reschedule",
        soft_fail=True,
        dag=dag,
    )

    trigger_secondary_dag_op = TriggerDagRunOperator(
        task_id="trigger_secondary_dag",
        trigger_dag_id="molecules_similarity_computation_pipeline_dag",
        trigger_rule="all_success",
        dag=dag,
    )

    finish_op = EmptyOperator(
        task_id="finish_check_for_files"
    )

    # Set the task dependencies
    start_op >> check_s3_bucket_op >> trigger_secondary_dag_op >> finish_op

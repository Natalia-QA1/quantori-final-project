import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from similarities.dfun_similarities_computation_pipeline import (
    get_source_molecules,
    get_target_molecules,
    compute_fingerprints
)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum

from airflow.operators.email import EmailOperator


default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="molecules_similarity_computation_pipeline_dag",
    start_date=pendulum.datetime(
        year=2024,
        month=7,
        day=1
    ),
    schedule=None,
    tags=[
            "aws_postgres",
            "s3",
            "pipeline",
            "similarities",
            "molecules_similarities_project"
        ],
    description="A DAG to take csv files with target molecules from s3 bucket, compute \
                for each target molecule similarity scores with the whole source dataset \
                and save the result in parquet files in s3 bucket. ",
    catchup=False
) as dag:

    start_op = EmptyOperator(
        task_id="start_chembl_similarities_computation_pipeline"
    )

    get_source_op = PythonOperator(
        task_id="get_source_molecules",
        callable=get_source_molecules
    )

    get_target_op = PythonOperator(
        task_id="get_target_molecules",
        callable=get_target_molecules
    )

    computation_op = PythonOperator(
        task_id="compute_fingerprints",
        callable=compute_fingerprints
    )

    finish_op = EmptyOperator(
        task_id="finish_similarity_computation_pipeline"
    )

    failure_notification_op = EmailOperator(
        task_id="send_email_on_failure",
        to="ananiev4nat@yandex.ru",
        subject="Airflow Task Failure: {{ task_instance.task_id }}",
        html_content="""<h3>Task Failed</h3>
                            <p>Task: {{ task_instance.task_id }}</p>
                            <p>DAG: {{ task_instance.dag_id }}</p>
                            <p>Execution Time: {{ execution_date }}</p>
                            <p>Log URL: <a href="{{ task_instance.log_url }}">{{ task_instance.log_url }}</a></p>""",
        trigger_rule="one_failed",
    )

    start_op >> [get_target_op, get_source_op] >> computation_op >> finish_op
    [get_target_op, get_source_op] >> failure_notification_op

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.email import EmailOperator


from airflow.models import (
    Param,
    Variable,
)
from datetime import datetime, timedelta
from .dfunc_top_10_similarities_pipeline import run_top_10pipeline


bucket_name = Variable.get("aws_s3_bucket_name")
file_prefix = Variable.get("s3_prefix_similarities")


default_args = {
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Define the primary DAG
with DAG(
    dag_id="top_10_similarities_computation_pipeline_dag",
    start_date=datetime(2023, 7, 1),
    schedule=None,
        tags=[
            "aws_postgres",
            "pipeline",
            "datamart",
            "s3",
            "molecules_similarities_project"
        ],
    description="A DAG triggeres by 's3_files_sensor_dag only if new files are \
                 presented in aws s3 bucket. This DAG retrievs parquet files, performs \
                 some transformation to compute top-10 similar source molecules and \
                 load result into aws Postgres database.",
    catchup=False,
    params={
        "pattern": Param(
            "r'similarity_scores_CHEMBL\d+\.parquet'",
            type="string"
        ),
    },
    default_args=default_args
) as dag:

    start_op = EmptyOperator(
        task_id="start_top_10_similarities_computation_pipeline"
    )

    run_pipeline_op = PythonOperator(
        task_id="run_top_10pipeline",
        python_callable=run_top_10pipeline
    )

    trigger_procedure_op = PostgresOperator(
        task_id="trigger_procedure",
        sql="call nananeva.insert_data_dm_top10_dim_molecules();",
        postgres_conn_id="aws_postgres"
    )

    finish_op = EmptyOperator(
        task_id="finish_top_10_similarities_computation_pipeline"
    )

    failure_notification = EmailOperator(
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

    start_op >> run_pipeline_op >> trigger_procedure_op >> finish_op
    [run_pipeline_op, trigger_procedure_op] >> failure_notification

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from dags_scripts.dfunc_dag_fingerprints_computation_pipeline import (
    create_chembl_mols_df,
    run_process
)

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import pendulum
# from sqlalchemy import create_engine
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.email import EmailOperator


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": "ananiev4nat@yandex.ru",
    "email_on_retry": False,
}

with DAG(
    dag_id="chembl_mols_fingerprint_computation_pipeline_dag",
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
            "fingerprints",
            "molecules_similarities_project"
        ],
    description="A DAG to run all chembl molecules fingerprints computation pipeline. \
                It loads data into Postgres 'st_fct_source_molecules_morgan_fingerprints' \
                table and save the result in parquet files in s3 bucket. ",
    default_args=default_args,
    catchup=False
) as dag:

    start_op = EmptyOperator(
        task_id="start_chembl_mols_fingerprint_computation_pipeline"
    )

    create_df_op = PythonOperator(
        task_id="create_chembl_mols_df",
        callable=create_chembl_mols_df
    )

    process_df_op = PythonOperator(
        task_id="run_process",
        callable=run_process
    )

    finish_op = EmptyOperator(
        task_id="finish_chembl_mols_fingerprint_computation_pipeline"
    )

    failure_email = EmailOperator(
        task_id="failure_email",
        to="ananiev4nat@yandex.ru",
        subject="Airflow DAG {{ dag.dag_id }} Failure",
        html_content="DAG {{ dag.dag_id }} has failed.",
        trigger_rule=TriggerRule.ONE_FAILED
    )

    trigger_secondary_dag_op = TriggerDagRunOperator(
        task_id="trigger_secondary_dag",
        trigger_dag_id="similarities_computation_pipeline_dag",
        trigger_rule="all_success",
        dag=dag,
    )

    start_op >> create_df_op >> process_df_op >> trigger_secondary_dag_op >> finish_op

    finish_op >> failure_email

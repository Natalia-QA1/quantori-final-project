import os
import sys

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from datetime import timedelta
import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

from scripts.dags_scripts.dfunc_dag_chembl_tables_pipeline import (
    check_total_rows_molecules,
    check_total_rows_lookups,
    load_chembl_id_lookup,
    load_molecule_dictionary,
    load_compound_properties,
    load_compound_structures
)
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": "ananiev4nat@yandex.ru",
    "email_on_retry": False,
    "sla": timedelta(hours=24),
}


with DAG(
        dag_id="chembl_tables_data_pipeline",
        start_date=pendulum.datetime(
            year=2024,
            month=7,
            day=1
        ),
        schedule_interval=None,
        tags=[
            "chembl_tables",
            "molecules_similarities_project"
        ],
        description="A DAG load data into DWH in aws Postgres database from chembl web service api.\
                     Tables: chembl_id_lookup, molecule_dictionary, compound_properties, compound_structures",
        catchup=False,
        dagrun_timeout=timedelta(
            minutes=5
        )
) as dag:

    start_op = EmptyOperator(
        task_id="start_chembl_tables_pipeline"
    )

    check_molecules_total_rows_op = PythonOperator(
        task_id="check_total_rows_molecules",
        python_callable=check_total_rows_molecules,
    )

    check_lookups_total_rows_op = PythonOperator(
        task_id="check_total_rows_lookups",
        python_callable=check_total_rows_lookups,
    )

    load_chembl_id_lookup_op = PythonOperator(
        task_id="load_chembl_id_lookup_table",
        python_callable=load_chembl_id_lookup,
    )

    load_molecule_dictionary_op = PythonOperator(
        task_id="load_molecule_dictionary_table",
        python_callable=load_molecule_dictionary,
    )

    load_compound_properties_op = PythonOperator(
        task_id="load_compound_properties_table",
        python_callable=load_compound_properties,
    )

    load_compound_structures_op = PythonOperator(
        task_id="load_compound_structures_table",
        python_callable=load_compound_structures,
    )

    finish_op = EmptyOperator(
        task_id="finish__pipeline",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    success_email = EmailOperator(
        task_id="success_email",
        to="ananiev4nat@yandex.ru",
        subject="Airflow DAG {{ dag.dag_id }} Success",
        html_content="DAG {{ dag.dag_id }} has succeeded.",
        trigger_rule=TriggerRule.ALL_SUCCESS
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
        trigger_dag_id="chembl_mols_fingerprint_computation_pipeline_dag",
        trigger_rule="all_success",
        dag=dag,
    )

    start_op >> check_molecules_total_rows_op
    start_op >> check_lookups_total_rows_op

    check_molecules_total_rows_op >> [
        load_molecule_dictionary_op,
        load_compound_properties_op,
        load_compound_structures_op
    ] >> trigger_secondary_dag_op >> finish_op

    check_lookups_total_rows_op >> load_chembl_id_lookup_op >> finish_op

    finish_op >> success_email
    finish_op >> failure_email

from airflow.models import Variable
from .dcode_dag_chembl_tables_pipeline import PostgresSourceTableLoader

from additional_scripts.chembl_tables_columns_keys_definition import (
    compound_properties_columns,
    compound_properties_keys_to_extract,
    compounds_structures_table_columns,
    compounds_structures_keys_to_extract,
    molecule_dictionary_keys_to_extract,
    molecule_dictionary_table_columns,
    lookup_ids_table_columns,
    lookup_ids_keys_to_extract
)
from additional_scripts.chembl_tables_definition_class import (
    CompoundProperties,
    CompoundStructures,
    ChemblLookupIds,
    MoleculesDictionary
)

loader = PostgresSourceTableLoader

url_molecules = Variable.get("molecules_api_url")
url_lookups = Variable.get("lookups_api_url")
lookup_api_key = Variable.get("lookup_api_key")
mol_api_key = Variable.get("mol_api_key")

def check_total_rows_lookups(ti):

    total_rows_lookups = loader.check_total_rows_amount(
        url_lookups
    )

    ti.xcom_push(
        key="total_rows_lookups_api",
        value=total_rows_lookups
    )

def check_total_rows_molecules(ti):

    total_rows_mol = loader.check_total_rows_amount(
        url_molecules
    )

    ti.xcom_push(
        key="total_rows_molecules_api",
        value=total_rows_mol
    )

def load_chembl_id_lookup(ti):

    total_rows = ti.xcom_pull(
        task_ids="check_total_rows_lookups",
        key="total_rows_lookups_api"
    )

    inst_loader = PostgresSourceTableLoader(
        url=url_lookups,
        table_col=lookup_ids_table_columns,
        total_rows=total_rows,
        table="st_dim_chembl_id_lookup",
        base_class=ChemblLookupIds,
        col_keys=lookup_ids_keys_to_extract,
        processes_num=4,
        batch_size=10000,
        api_key=lookup_api_key
    )

    inst_loader.parallel_fetch_and_insert()

def load_molecule_dictionary(ti):

    total_rows = ti.xcom_pull(
        task_ids="check_total_rows_molecules",
        key="total_rows_molecules_api"
    )

    inst_loader = PostgresSourceTableLoader(
        url=url_molecules,
        table_col=molecule_dictionary_table_columns,
        total_rows=total_rows,
        table="st_dim_molecule_dictionary",
        base_class=MoleculesDictionary,
        col_keys=molecule_dictionary_keys_to_extract,
        processes_num=4,
        batch_size=5000,
        api_key=mol_api_key
    )

    inst_loader.parallel_fetch_and_insert()

def load_compound_properties(ti):

    total_rows = ti.xcom_pull(
        task_ids="check_total_rows_molecules",
        key="total_rows_molecules_api"
    )

    inst_loader = PostgresSourceTableLoader(
        url=url_molecules,
        table_col=compound_properties_columns,
        table="st_dim_compound_properties",
        total_rows=total_rows,
        base_class=CompoundProperties,
        col_keys=compound_properties_keys_to_extract,
        processes_num=4,
        batch_size=5000,
        api_key=mol_api_key
    )
    inst_loader.parallel_fetch_and_insert()

def load_compound_structures(ti):

    total_rows = ti.xcom_pull(
        task_ids="check_total_rows_molecules",
        key="total_rows_molecules_api"
    )

    inst_loader = PostgresSourceTableLoader(
        url=url_molecules,
        table_col=compounds_structures_table_columns,
        table="st_dim_compound_structures",
        total_rows=total_rows,
        base_class=CompoundStructures,
        col_keys=compounds_structures_keys_to_extract,
        processes_num=4,
        batch_size=5000,
        api_key=mol_api_key
    )

    inst_loader.parallel_fetch_and_insert()

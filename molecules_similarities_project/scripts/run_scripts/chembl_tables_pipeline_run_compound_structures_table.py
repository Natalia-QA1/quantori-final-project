from additional_scripts.chembl_tables_columns_keys_definition import (
    compounds_structures_keys_to_extract,
    compounds_structures_table_columns
)
from additional_scripts.chembl_tables_definition_class import CompoundStructures
from modules.chembl_tables_pipeline_main_module import PostgresSourceTableLoader


def main():
    loader = PostgresSourceTableLoader(
        url="https://www.ebi.ac.uk/chembl/api/data/molecule.json",
        table_col=compounds_structures_table_columns,
        table="st_dim_compound_properties",
        base_class=CompoundStructures,
        col_keys=compounds_structures_keys_to_extract,
        processes_num=5,
        batch_size=5000,
        api_key="molecules"
    )

    loader.parallel_fetch_and_insert()


if __name__ == '__main__':
    main()

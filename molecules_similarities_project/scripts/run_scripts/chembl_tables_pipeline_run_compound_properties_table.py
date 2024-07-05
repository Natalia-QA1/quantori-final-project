from additional_scripts.chembl_tables_columns_keys_definition import (
    compound_properties_columns,
    compound_properties_keys_to_extract
)
from additional_scripts.chembl_tables_definition_class import CompoundProperties
from modules.chembl_tables_pipeline_main_module import PostgresSourceTableLoader


def main():
    loader = PostgresSourceTableLoader(
        url="https://www.ebi.ac.uk/chembl/api/data/molecule.json",
        table_col=compound_properties_columns,
        table="st_dim_compound_properties",
        base_class=CompoundProperties,
        col_keys=compound_properties_keys_to_extract,
        processes_num=5,
        batch_size=5000,
        api_key="molecules"
    )

    loader.parallel_fetch_and_insert()


if __name__ == '__main__':
    main()

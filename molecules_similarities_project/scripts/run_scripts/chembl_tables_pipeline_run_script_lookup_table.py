from additional_scripts.chembl_tables_columns_keys_definition import (
    lookup_ids_table_columns,
    lookup_ids_keys_to_extract
)
from additional_scripts.chembl_tables_definition_class import ChemblLookupIds
from modules.chembl_tables_pipeline_main_module import PostgresSourceTableLoader


def main():
    loader = PostgresSourceTableLoader(
        url="https://www.ebi.ac.uk/chembl/api/data/chembl_id_lookup.json",
        table_col=lookup_ids_table_columns,
        table="st_dim_chembl_id_lookup_",
        base_class=ChemblLookupIds,
        col_keys=lookup_ids_keys_to_extract,
        processes_num=5,
        batch_size=5000,
        api_key="chembl_id_lookups"
    )

    loader.parallel_fetch_and_insert()


if __name__ == '__main__':
    main()

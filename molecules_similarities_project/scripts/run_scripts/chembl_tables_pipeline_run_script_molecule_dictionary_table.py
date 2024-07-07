from additional_scripts.chembl_tables_columns_keys_definition import (
    molecule_dictionary_table_columns,
    molecule_dictionary_keys_to_extract
)
from additional_scripts.chembl_tables_definition_class import MoleculesDictionary
from modules.chembl_tables_pipeline_main_module import PostgresSourceTableLoader


def main():
    loader = PostgresSourceTableLoader(
        url="https://www.ebi.ac.uk/chembl/api/data/molecule.json",
        table_col=molecule_dictionary_table_columns,
        table="st_dim_molecule_dictionary",
        base_class=MoleculesDictionary,
        col_keys=molecule_dictionary_keys_to_extract,
        processes_num=5,
        batch_size=5000,
        api_key="molecules"
    )

    loader.parallel_fetch_and_insert()


if __name__ == '__main__':
    main()

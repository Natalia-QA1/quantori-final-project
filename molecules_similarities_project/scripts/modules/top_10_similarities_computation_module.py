import pandas as pd
from additional_scripts.chembl_tables_definition_class import MoleculeSimilarity
from config.config import (
    PG_USER,
    PG_PASSWORD,
    PG_DATABASE,
    PG_HOST,
    PG_PORT
)
from modules.aws_postgres_connector_module import PostgresConnector
from modules.s3_connector_module import S3DataDownloader


class Top10SimilaritiesGenerator:

    def __init__(self):
        self.get_data = S3DataDownloader(
            "de-school-2024-aws",
            "final_task/ananeva_natalia/similarities/",
            r'similarity_scores_CHEMBL\d+\.parquet'
        )

    def download_parquet_with_target_mols_to_df(self) -> list[pd.DataFrame]:
        """
        The method connects to Amazon s3 bucket and download parquet files.
        :return: A list of DataFrames (one DataFrame for each file).
        """

        target_mols_dfs = self.get_data.download_parquet_data()

        column_patterns_mols_df = {
            "target_chembl_id ": [r'target_chembl_id'],
            "source_chembl_id  ": [r'source_chembl_id  '],
            "similarity_score  ": [r'similarity_score  ']
        }

        return target_mols_dfs

    @staticmethod
    def process_chunk(chunk):
        """
        Processes a chunk of data to find the top 10 similar molecules and
        mark duplicates. Then loads result set into Postgres database.
        :param chunk: A DataFrame containing similarities for one target
               molecule with all source molecules
        :return:
        """
        def top_10_with_duplicates(group):
            """
            Processes a chunk of data to find the top 10 similar molecules
            and mark duplicates in case of other source molecules have the
            same similarity score as the 10th top source molecule.
            :param group: A DataFrame group corresponding to a specific
                   target molecule.
            :return: A DataFrame containing the top 10 similar molecules
                   with a flag for duplicates.
            """
            top_10_df = group.nlargest(
                10,
                "similarity_score"
            )

            last_largest_score = top_10_df['similarity_score'].iloc[-1]

            df_sorted = group.sort_values(
                by='similarity_score',
                ascending=False
            ).head(11)

            check_duplicates = df_sorted['similarity_score'].iloc[-1]

            if check_duplicates == last_largest_score:
                top_10_df['has_duplicates_of_last_largest_score'] = True
            else:
                top_10_df['has_duplicates_of_last_largest_score'] = False

            return top_10_df

        chunk = chunk[chunk["target_chembl_id"] != chunk["source_chembl_id"]]

        top_10_chunk = chunk.groupby(
            "target_chembl_id",
            group_keys=False
        ).apply(top_10_with_duplicates).reset_index(
            drop=True
        )

        top_10_chunk.rename(
            columns={
                "target_chembl_id": "target_molecule_reference",
                "source_chembl_id": "source_molecule_reference",
                "similarity_score": "tanimoto_similarity_score"
            },
            inplace=True
        )

        pg_connector = PostgresConnector(
            PG_DATABASE,
            PG_HOST,
            PG_USER,
            PG_PASSWORD,
            PG_PORT
        )
        pg_connector.connect_for_bulk_load(
            top_10_chunk,
            MoleculeSimilarity,
            "dm_top10_fct_molecules_similarities"
        )


def process_dfs_chunk(dfs_chunk):
    """
    The method processes a list of DataFrame chunks,
    applying the top 10 similarity calculation
    :param dfs_chunk: List of DataFrame chunks to be processed.
    :return:
    """

    for df in dfs_chunk:
        Top10SimilaritiesGenerator.process_chunk(df)

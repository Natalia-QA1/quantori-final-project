import logging

import numpy as np
import pandas as pd
from airflow.models import Variable
from modules.aws_postgres_connector_module import PostgresConnector
from modules.m_rdkit_computations import (
    MorganFingerprintGenerator,
    TanimotoSimilarityForMoleculesFromAWS
from airflow.providers.postgres.hooks.postgres import PostgresHook

from m_s3_connector_module import (
    S3DataLoader,
    S3DataDownloader
)

pg_hook = PostgresHook(
    postgres_conn_id="aws_postgres"
)

conn_params = pg_hook.get_connection(
    pg_hook.conn_id
)

pg_connector = PostgresConnector(
    pg_database=conn_params.schema,
    pg_host=conn_params.host,
    pg_user=conn_params.login,
    pg_password=conn_params.password,
    pg_port=conn_params.port
)

bucket_name = Variable.get("aws_s3_bucket_name")
folder= Variable.get("aws_s3_prefix_input")
pattern = Variable.get("input_csv_pattern")


class TargetMoleculesSimilarityGeneratorBaseException(
    Exception
):
    pass


class TargetMoleculesSimilarityGeneratorKeyValueError(
    TargetMoleculesSimilarityGeneratorBaseException
):
    pass

class TargetMoleculesSimilarityGenerator:

    def __init__(self):
        self.connector = PostgresConnector(
            pg_database=conn_params.schema,
            pg_host=conn_params.host,
            pg_user=conn_params.login,
            pg_password=conn_params.password,
            pg_port=conn_params.port
        )
        self.get_data = S3DataDownloader(
            bucket_name,
            folder,
            pattern
        )
        self.bucket_name = bucket_name
        self.tanimoto_generator = TanimotoSimilarityForMoleculesFromAWS()
        self.s3_loader = S3DataLoader()

    def download_csv_with_target_mols_to_df(self) -> pd.DataFrame:

        target_mols_dfs = self.get_data.download_csv_data(
            "utf-8"
        )
        column_patterns_mols_df = {
            "molecule_name": [r'^molecule[_\s]?name$'],
            "smiles": [r'^smiles$']
        }

        target_mols = self.get_data.combine_data(
            target_mols_dfs,
            column_patterns_mols_df)

        return target_mols

    def download_source_mols_from_fingerprints_table_postgres(self) -> pd.DataFrame:

        query = """
            SELECT chembl_id, canonical_smiles
            FROM nananeva.st_dim_compound_structures
            """

        rows = self.connector.connect_conn(
            query
        )

        chembl_mols = pd.DataFrame(
            rows,
            columns=[
                "chembl_id",
                "canonical_smiles"
            ]
        )

        return chembl_mols

    def compute_fingerprints_for_chembl_mols(self, chembl_mols, chunk_size=100000):

        chembl_mols["fingerprint"] = chembl_mols["canonical_smiles"].apply(
            lambda x: MorganFingerprintGenerator.compute_fingerprint(x) if x else None
        )

        chembl_mols = chembl_mols.dropna(subset=['fingerprint'])
        logging.info(
            f"Source molecules with fingerprints: {len(chembl_mols)}"
        )

        chembl_chunks = np.array_split(chembl_mols, len(chembl_mols) // chunk_size + 1)

        return chembl_chunks

    def compute_similarities(self, target_mols, chembl_chunks):

        target_mols["fingerprint"] = target_mols["smiles"].apply(
            lambda x: MorganFingerprintGenerator.compute_fingerprint(x) if x else None
        )

        target_mols = target_mols.dropna(
            subset=["fingerprint"]
        )
        logging.info(
            f"Target molecules with fingerprints to compute similarity: {len(target_mols)}"
        )

        all_similarity_scores = []

        for chunk in chembl_chunks:
            similarity_scores_chunk = self.tanimoto_generator.compute_similarity_scores(
                target_mols,
                chunk
            )

            all_similarity_scores.extend(
                similarity_scores_chunk
            )

        rows = []

        for entry in all_similarity_scores:
            target_chembl_id = entry["target_chembl_id"]
            source_chembl_ids = entry["source_chembl_id"]
            similarity_scores = entry["similarity_score"]

            for source_chembl_id, similarity_score in zip(source_chembl_ids, similarity_scores):
                rows.append({
                    "target_chembl_id": target_chembl_id,
                    "source_chembl_id": source_chembl_id,
                    "similarity_score": similarity_score
                })

        df_similarity_scores = pd.DataFrame(rows)

        logging.info(
            "Similarity scores dataframe created."
        )

        return df_similarity_scores

    def save_similarity_scores_to_s3(self, df, folder_name):

        if df.empty:
            logging.warning(f"The DataFrame is empty.")
            return

        try:
            if "target_chembl_id" not in df.columns:
                raise TargetMoleculesSimilarityGeneratorKeyValueError(
                    "target_chembl_id column is missing in the DataFrame"
                )

            unique_target_ids = df["target_chembl_id"].unique()

            for target_chembl_id in unique_target_ids:

                target_df = df[df["target_chembl_id"] == target_chembl_id]

                parquet_file = f"similarity_scores_{target_chembl_id}.parquet"
                target_df.to_parquet(
                    parquet_file,
                    index=False
                )

                self.s3_loader.upload_data(
                    parquet_file,
                    self.bucket_name,
                    folder_name
                )

                logging.info(
                    f"Uploaded {parquet_file} to s3://{self.bucket_name}/{folder_name}/{parquet_file}"
                )

        except KeyError as e:
            raise TargetMoleculesSimilarityGeneratorKeyValueError(
                f"Key error: {e}"
            )
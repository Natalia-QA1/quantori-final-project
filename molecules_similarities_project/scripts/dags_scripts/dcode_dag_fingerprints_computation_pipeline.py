import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from additional_scripts.chembl_tables_definition_class import MorganFingerprint
from airflow.providers.postgres.hooks.postgres import PostgresHook
from modules.aws_postgres_connector_module import PostgresConnector
from modules.rdkit_computations_module import MorganFingerprintGenerator
from modules.s3_connector_module import S3DataLoader

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

s3_loader = S3DataLoader()

fingerprint_generator = MorganFingerprintGenerator()


class ChemblMoleculesFingerprintGeneratorBaseException(
    Exception
):
    pass


class ChemblMoleculesFingerprintGeneratorComputationError(
    ChemblMoleculesFingerprintGeneratorBaseException
):
    pass


class ChemblMoleculesFingerprintGeneratorMemoryError(
    ChemblMoleculesFingerprintGeneratorBaseException
):
    pass


class ChemblMoleculesFingerprintGenerator:

    def create_df(self):

        query = """
        SELECT chembl_id, canonical_smiles 
        FROM nananeva.st_dim_compound_structures
        WHERE canonical_smiles IS NOT NULL
        """

        rows = pg_connector.connect_conn(query)
        df = pd.DataFrame(rows, columns=[
            "chembl_id",
            "canonical_smiles"
        ])

        return df

    def process_chunk(self, args):
        chunk, chunk_index, bucket_name, folder_name, table_to_insert = args

        def safe_compute_fingerprint(x):
            try:
                fingerprint = fingerprint_generator.compute_fingerprint(
                    x,
                    2,
                    2048
                )
                if fingerprint:
                    return fingerprint.encode()
                else:
                    return None

            except (ValueError, TypeError, OverflowError, IOError) as e:
                raise ChemblMoleculesFingerprintGeneratorComputationError(
                    f"Error computing fingerprint for {x}: {e}"
                )
            except MemoryError as me:
                raise ChemblMoleculesFingerprintGeneratorMemoryError(
                    f"MemoryError computing fingerprint for {x}: {me}"
                )

        chunk["morgan_fingerprint"] = chunk["canonical_smiles"].apply(
            safe_compute_fingerprint
        )

        chunk = chunk.dropna(subset=[
            "morgan_fingerprint"
        ])
        chunk.drop(columns=["canonical_smiles"],
                   inplace=True
                   )

        logging.info(
            f"Processing chunk {chunk_index}: {len(chunk)} rows"
        )

        pg_connector.connect_for_bulk_load(
            chunk,
            MorganFingerprint,
            table_to_insert
        )

        parquet_file = f"fingerprints_chunk_{chunk_index}.parquet"
        table = pa.Table.from_pandas(chunk)
        pq.write_table(
            table,
            parquet_file
        )

        if bucket_name and folder_name:
            s3_loader.upload_data(
                parquet_file,
                bucket_name,
                folder_name,
                object_name=None
            )

        logging.info(
            f"Chunk {chunk_index} processed and saved to \
            {bucket_name}/{folder_name}/{parquet_file}"
        )

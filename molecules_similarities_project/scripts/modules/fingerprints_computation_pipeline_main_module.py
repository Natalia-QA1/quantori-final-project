import logging

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from aws_postgres_connector import PostgresConnector
from chembl_tables_pipeline.chembl_tables_definition_class import MorganFingerprint
from config.config import PG_DATABASE, PG_HOST, PG_USER, PG_PASSWORD, PG_PORT
from rdkit_computations import MorganFingerprintGenerator
from s3_connector import S3DataLoader

logging.basicConfig(
    level=logging.INFO
)

connector = PostgresConnector(
    PG_DATABASE,
    PG_HOST,
    PG_USER,
    PG_PASSWORD,
    PG_PORT
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
        rows = connector.connect_conn(query)

        df = pd.DataFrame(
            rows,
            columns=[
                "chembl_id",
                "canonical_smiles"
            ])

        return df

    def process_chunk(self, args):
        """
        The method processes a subset of data, generate molecular fingerprints,
        and store the processed data in a PostgreSQL database and an S3 bucket.
        :param args:A tuple containing:
            chunk: The pandas DataFrame chunk to process.
            chunk_index: The index of the current chunk.
            bucket_name: The name of the S3 bucket where the processed data will
            be uploaded.
            folder_name: The folder within the S3 bucket.
            table_to_insert: The name of the database table where the processed
            data will be inserted.
        :return:
        """
        chunk, chunk_index, bucket_name, folder_name, table_to_insert = args

        def safe_compute_fingerprint(x):
            """
            The method is a helper function designed to safely compute molecular
            fingerprints for chemical structures. It handles various exceptions
            to ensure robust and error-free fingerprint generation.
            :param x: A string representing the chemical structure in the form
            of a canonical SMILES (Simplified Molecular Input Line Entry System) string.
            """
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

        connector.connect_for_bulk_load(
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
            f"Chunk {chunk_index} processed and saved to" 
            f"{bucket_name}/{folder_name}/{parquet_file}"
        )

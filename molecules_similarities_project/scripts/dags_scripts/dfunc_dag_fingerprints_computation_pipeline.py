from .dcode_dag_fingerprints_computation_pipeline import ChemblMoleculesFingerprintGenerator
import logging
import numpy as np
import multiprocessing as mp
from airflow.models import Variable

num_chunks = Variable.get("process_number")

generator = ChemblMoleculesFingerprintGenerator()


def create_chembl_mols_df(ti):

    chembl_mols_df = generator.create_df()
    logging.info(
        f"Total chembl molecules to process: {len(chembl_mols_df)}"
    )
    ti.xcom_push(
        key="chembl_mols_df",
        value=chembl_mols_df
    )


def run_process(ti, num_chunks):

    df = ti.xcom_pull(
        task_ids="create_chembl_mols_df",
        key="chembl_mols_df"
    )
    chunks = np.array_split(
        df,
        num_chunks
    )

    bucket_name = Variable.get("bucket_name")   # CREATe
    folder_name = Variable.get("folder_name")
    table_to_insert = "st_fct_source_molecules_morgan_fingerprints"

    args = [(chunks[i], i, bucket_name, folder_name, table_to_insert) for i in range(num_chunks)]

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(
            generator.process_chunk,
            args
        )

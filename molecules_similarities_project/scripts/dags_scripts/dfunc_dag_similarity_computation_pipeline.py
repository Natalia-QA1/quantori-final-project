import logging
from .dcode_dag_similarities_computation_pipeline import TargetMoleculesSimilarityGenerator
from airflow.models import Variable
import numpy as np


generator = TargetMoleculesSimilarityGenerator()

bucket_name = Variable.get("aws_s3_bucket_name")
folder_name = Variable.get("s3_prefix_similarities")

def get_target_molecules(ti):

    target_mols = generator.download_csv_with_target_mols_to_df()

    logging.info(
        f"Initial molecules amount to process: {len(target_mols)}"
    )

    target_mols = target_mols[target_mols["molecule_name"].str.startswith("CHEMBL")]
    target_mols = target_mols.drop_duplicates()

    logging.info(
        f"Molecules amount after cleaning: {len(target_mols)}"
    )

    ti.xcom_push(
        key="target_mols_df",
        value=target_mols
    )

def get_source_molecules(ti):

    chembl_mols = generator.download_source_mols_from_fingerprints_table_postgres()
    logging.info(
        f"Amount of source molecules: {len(chembl_mols)}"
    )

    ti.xcom_push(
        key="source_mols_df",
        value=chembl_mols
    )

def compute_fingerprints(ti):

    target_mols = ti.xcom_pull(
        task_ids="get_target_molecules",
        key="target_mols_df"
    )

    chembl_mols = ti.xcom_pull(
        task_ids="get_source_molecules",
        key="source_mols_df"
    )

    chembl_chunks = generator.compute_fingerprints_for_chembl_mols(
        chembl_mols
    )

    num_chunks = len(target_mols)
    target_mols_chunks = np.array_split(target_mols, num_chunks)

    for target_mol in target_mols_chunks:
        similarity_df = generator.compute_similarities(
            target_mols=target_mol,
            chembl_chunks=chembl_chunks
        )

        generator.save_similarity_scores_to_s3(
            similarity_df,
            bucket_name,
            folder_name
        )

# TODO: implement multiprocessing!
import logging
import numpy as np
from modules.similarities_computation_pipeline_module import TargetMoleculesSimilarityGenerator


def main():

    generator = TargetMoleculesSimilarityGenerator()

    target_mols = generator.download_csv_with_target_mols_to_df()

    logging.info(
        f"Initial molecules amount to process: {len(target_mols)}"
    )

    target_mols = target_mols[target_mols["molecule_name"].str.startswith("CHEMBL")]

    target_mols = target_mols.drop_duplicates()

    logging.info(
        f"Molecules amount after cleaning: {len(target_mols)}"
    )

    chembl_mols = generator.download_source_mols_from_fingerprints_table_postgres()

    logging.info(
        f"Amount of source molecules: {len(chembl_mols)}"
    )

    chembl_chunks = generator.compute_fingerprints_for_chembl_mols(
        chembl_mols
    )

    num_chunks = len(chembl_chunks)
    target_mols_chunks = np.array_split(
        target_mols,
        num_chunks
    )

    bucket_name = "de-school-2024-aws"
    folder_name = "final_task/ananeva_natalia/similarities"

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


if __name__ == "__main__":
    main()

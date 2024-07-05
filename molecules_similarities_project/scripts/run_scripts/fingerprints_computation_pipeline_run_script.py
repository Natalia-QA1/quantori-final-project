import logging
import multiprocessing as mp

import numpy as np
from modules.fingerprints_computation_pipeline_main_module import \
    ChemblMoleculesFingerprintGenerator


def main():
    generator = ChemblMoleculesFingerprintGenerator()
    df = generator.create_df()
    logging.info(
        f"Total rows to process: {len(df)}"
    )

    num_chunks = 5
    chunks = np.array_split(
        df,
        num_chunks
    )

    bucket_name = "de-school-2024-aws"
    folder_name = "final_task/ananeva_natalia/"
    table_to_insert = "st_fct_source_molecules_morgan_fingerprints"

    args = [(chunks[i], i, bucket_name, folder_name, table_to_insert) for i in range(num_chunks)]

    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = pool.map(
            generator.process_chunk,
            args
        )


if __name__ == "__main__":
    main()

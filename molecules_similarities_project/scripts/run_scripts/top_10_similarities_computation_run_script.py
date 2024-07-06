import multiprocessing as mp

from modules.top_10_similarities_computation_module import Top10SimilaritiesGenerator


def main():
    generator = Top10SimilaritiesGenerator()

    target_mols_dfs = generator.download_parquet_with_target_mols_to_df()

    num_chunks = mp.cpu_count()
    chunk_size = len(target_mols_dfs) // num_chunks

    chunks = [target_mols_dfs[i:i + chunk_size] for i in range(
        0,
        len(target_mols_dfs),
        chunk_size
    )]

    with mp.Pool(num_chunks) as pool:
        pool.map(
            generator.process_dfs_chunk,
            chunks
        )


if __name__ == '__main__':
    main()

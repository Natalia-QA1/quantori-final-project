import multiprocessing as mp

from .dcode_top_10_similarities_computation_module import Top10SimilaritiesGenerator

from concurrent.futures import ProcessPoolExecutor


def process_dfs_chunk(dfs_chunk):

    for df in dfs_chunk:
        Top10SimilaritiesGenerator.process_chunk(df)


def run_top_10pipeline():

    generator = Top10SimilaritiesGenerator()
    target_mols_dfs = generator.download_parquet_with_target_mols_to_df()

    num_chunks = mp.cpu_count()
    chunk_size = max(1, len(target_mols_dfs) // num_chunks)

    chunks = [target_mols_dfs[i:i + chunk_size] for i in range(0, len(target_mols_dfs), chunk_size)]

    with ProcessPoolExecutor(max_workers=num_chunks) as executor:
        executor.map(process_dfs_chunk, chunks)

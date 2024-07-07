from additional_scripts.chembl_tables_definition_class import MoleculeSimilarity

from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from modules.aws_postgres_connector_module import PostgresConnector

from .s3_connector_module import S3DataDownloader

bucket_name = Variable.get("aws_s3_bucket_name")
folder = Variable.get("similarities_s3_folder")
pattern = Variable.get("similarities_parquet_pattern")


class Top10SimilaritiesGenerator:

    def __init__(self):
        self.get_data = S3DataDownloader(
            bucket_name,
            folder,
            pattern
        )

    def download_parquet_with_target_mols_to_df(self):

        target_mols_dfs = self.get_data.download_parquet_data()

        return target_mols_dfs


    def process_chunk(self, chunk):

        def top_10_with_duplicates(group):

            top_10_df = group.nlargest(
                10,
                "similarity_score"
            )

            last_largest_score = top_10_df["similarity_score"].iloc[-1]

            df_sorted = group.sort_values(
                by="similarity_score",
                ascending=False
            ).head(11)

            check_duplicates = df_sorted["similarity_score"].iloc[-1]

            if check_duplicates == last_largest_score:
                top_10_df["has_duplicates_of_last_largest_score"] = True
            else:
                top_10_df["has_duplicates_of_last_largest_score"] = False

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

        pg_connector.connect_for_bulk_load(
            top_10_chunk,
            MoleculeSimilarity,
            "dm_top10_fct_molecules_similarities"
        )

import logging
import time
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import requests
from airflow.hooks.base_hook import BaseHook
from modules.aws_postgres_connector_module import PostgresConnector
from requests import RequestException


class PostgresSourceTableLoaderBaseException(Exception):
    pass


class PostgresSourceTableLoaderNetworkException(
    PostgresSourceTableLoaderBaseException
):
    pass


class PostgresSourceTableLoaderDataIntegrityException(
    PostgresSourceTableLoaderBaseException
):
    pass


class PostgresSourceTableLoaderGeneralException(
    PostgresSourceTableLoaderBaseException
):
    pass


class PostgresSourceTableLoader:

    def __init__(self, url, table_col, table, total_rows, base_class, col_keys, processes_num,
                 batch_size, api_key):
        self.base_url = url
        self.table_columns = table_col
        self.table = table
        self.total_rows = total_rows
        self.base_class = base_class
        self.keys_to_extract = col_keys
        self.processes_num = processes_num
        self.batch_size = batch_size
        self.api_key = api_key

        # self.hook = CustomPostgresHook(postgres_conn_id=conn_id)
        # connection = self.hook.get_connection(conn_id)
        # logging.info(f"Connection details: {connection.get_uri()}")
        # self.engine = self.hook.get_sqlalchemy_engine()
        # self.Session = sessionmaker(bind=self.engine)

    @staticmethod
    def check_total_rows_amount(url):

        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if "page_meta" in data and "total_count" in data["page_meta"]:
                total_rows_count = data["page_meta"]["total_count"]
                logging.info(
                    f"Total count: {total_rows_count}"
                )
                return total_rows_count
            else:
                logging.info(
                    "Unexpected response structure: {data}"
                )
                return None
        except (
                requests.HTTPError,
                requests.ConnectionError,
                requests.Timeout
        ) as conn_err:
            raise PostgresSourceTableLoaderNetworkException(
                f"HTTPError occurred: {conn_err}."
            )

    def fetch_data(self, start, end, retries=5, delay=2):
        result = []
        for offset in range(start, end, self.batch_size):
            for attempt in range(retries):
                try:
                    response = requests.get(
                        self.base_url,
                        params={
                            "limit": self.batch_size,
                            "offset": offset
                        }
                    )
                    response.raise_for_status()
                    data = response.json().get(
                        self.api_key, []
                    )
                    if not data:
                        break
                    result.extend(data)
                    break

                except RequestException as conn_e:
                    logging.info(
                        f"Error fetching data: {conn_e}"
                    )
                    time.sleep(delay * (2 ** attempt))
            else:
                raise PostgresSourceTableLoaderNetworkException(
                    "Max retries reached. Skipping to next batch."
                )
        return result

    def process_mol_batch(self, molecules, keys):
        def extract_nested(data, key_path):

            keys = key_path.split('.')
            current_data = data

            for key in keys:
                if isinstance(current_data, dict):
                    current_data = current_data.get(key)
                elif isinstance(current_data, list):
                    try:
                        index = int(key)
                        current_data = current_data[index]
                    except (IndexError, ValueError):
                        current_data = None
                else:
                    current_data = None
                if current_data is None:
                    return None
            return current_data

        filtered_data = []
        for mol in molecules:
            entry = {}
            for key in keys:
                if isinstance(key, tuple):
                    value = extract_nested(mol, key[0])
                    entry[key[1]] = value
                else:
                    entry[key] = mol.get(key)
            filtered_data.append(entry)

        return pd.DataFrame(filtered_data)

    def ensure_column_order_and_types(self, df):

        for col, dtype in self.table_columns.items():
            if col not in df.columns:
                df[col] = None
            if dtype == "str":
                df[col] = df[col].astype("object")
            elif dtype == "float":
                df[col] = df[col].astype("float")

        df = df[self.table_columns.keys()]

        return df

    def convert_bool_to_numeric(self, df):

        bool_columns = [
            col for col, dtype in self.table_columns.items() if dtype == "bool"
        ]
        for col in bool_columns:
            df[col] = df[col].astype(float)

        return df

    def fetch_and_insert(self, start, end):
        logging.info(
            f"Process starting for range {start} to {end}"
        )

        try:
            for offset in range(start, end, self.batch_size):
                logging.info(
                    f"Fetching data from offset {offset} to "
                    f"{min(offset + self.batch_size, end)}"
                )

                data = self.fetch_data(
                    offset,
                    min(offset + self.batch_size, end),
                    self.batch_size
                )

                if not data:
                    logging.warning(
                        f"No data fetched for offset {offset} to \
                        {min(offset + self.batch_size, end)}")
                    continue

                all_mol_param = [molecule for molecule in data]

                mols_df = self.process_mol_batch(
                    all_mol_param,
                    self.keys_to_extract)

                mols_df_check_col = self.ensure_column_order_and_types(
                    mols_df
                )

                mols_df = self.convert_bool_to_numeric(
                    mols_df_check_col
                )

                mols_df.rename(
                    columns={
                        "molecule_chembl_id": "chembl_id"
                    },
                    inplace=True
                )

                filtered_df = mols_df[mols_df["chembl_id"].notna()]

                connection = BaseHook.get_connection("aws_postgres")

                pg_database = connection.schema
                pg_host = connection.host
                pg_user = connection.login
                pg_password = connection.password
                pg_port = connection.port

                pg_connector = PostgresConnector(
                    pg_database,
                    pg_host,
                    pg_user,
                    pg_password,
                    pg_port
                )

                pg_connector.connect_for_bulk_load(
                    filtered_df,
                    self.base_class,
                    self.table
                )

                logging.info(
                    f"Inserted {len(filtered_df)} rows into DB." 
                    f"Total rows: {offset + len(filtered_df)}"
                )
        except (TypeError, ValueError) as e:
            raise PostgresSourceTableLoaderGeneralException(
                f"Error during processing occurred: {e}"
            )

        logging.info(
            f"Process finished for range {start} to {end}"
        )

    def parallel_fetch_and_insert(self):

        with ThreadPoolExecutor(
                max_workers=self.processes_num
        ) as executor:

            futures = []
            rows_per_process = self.total_rows // self.processes_num

            for i in range(self.processes_num):
                start = i * rows_per_process
                end = start + rows_per_process if i != self.processes_num - 1 else self.total_rows
                future = executor.submit(self.fetch_and_insert, start, end)
                futures.append(future)

            for future in futures:
                future.result()

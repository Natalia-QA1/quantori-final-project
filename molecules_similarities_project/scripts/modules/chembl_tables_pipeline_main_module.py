import logging
import random
import time
from multiprocessing import Process

import pandas as pd
import requests
from molecules_similarities_project.config.config import (
    PG_USER,
    PG_PASSWORD,
    PG_DATABASE,
    PG_HOST,
    PG_PORT
)
from requests import RequestException
from requests.exceptions import (
    HTTPError,
    ConnectionError,
    Timeout
)
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker
from aws_postgres_connector_module import PostgresConnector
logging.basicConfig(
    level=logging.INFO
)

class PostgresSourceTableLoaderBaseException(
    Exception
):
    pass


class PostgresSourceTableLoaderNetworkException(
    PostgresSourceTableLoaderBaseException
):
    pass


class PostgresSourceTableLoaderDataIntegrityException(
    PostgresSourceTableLoaderBaseException
):
    pass


class PostgresSourceTableLoaderUnexpectedResponseStructureException(
    PostgresSourceTableLoaderBaseException
):
    pass


class PostgresSourceTableLoaderGeneralException(
    PostgresSourceTableLoaderBaseException
):


class PostgresSourceTableLoader:

    """
    The PostgresSourceTableLoader class is designed to
    facilitate the loading of data from an external API
    source into a PostgreSQL database.
    """
    def __init__(
            self,
            url,
            table_col,
            table,
            base_class,
            col_keys,
            processes_num,
            batch_size,
            api_key
    ):
        self.base_url = url
        self.table_columns = table_col
        self.table = table
        self.base_class = base_class
        self.keys_to_extract = col_keys
        self.processes_num = processes_num
        self.batch_size = batch_size
        self.total_rows = self.check_total_rows_amount()
        self.api_key = api_key

    def check_total_rows_amount(self):
        """
        The method fetches dataset rows amount from api to generate batches in
        multiprocessing.
        :return: rows count
        """

        try:
            response = requests.get(
                self.base_url
            )
            response.raise_for_status()
            data = response.json()
            if "page_meta" in data and "total_count" in data["page_meta"]:
                total_rows_count = data["page_meta"]["total_count"]
                logging.info(
                    f"Total row count for {self.base_url} is {total_rows_count}."
                )
                return total_rows_count
            else:
                raise PostgresSourceTableLoaderUnexpectedResponseStructureException(
                    f"Unexpected response structure: {data}"
                )

        except (HTTPError, ConnectionError, Timeout) as conn_err:
            raise PostgresSourceTableLoaderNetworkException(
                f"Network error occurred: {conn_err}."
            )

    def fetch_data( self,start,end,batch_size=5000,retries=5, delay=2,result=None):
        """
        The method data from an external API in batches.
        It employs a retry mechanism to handle transient errors and ensures that
        data fetching can continue even if some requests fail.
        :param start: The starting offset for fetching data.
        :param end: The ending offset for fetching data.
        :param batch_size: The number of records to fetch in each batch.
               Default is 5000.
        :param retries: The number of retry attempts in case of a request failure.
               Default is 5.
        :param delay: The delay between retry attempts, in seconds.
               Default is 2 seconds.
        :param result:
        :return: A list to store the fetched data.
               Default is None.
        """
        if result is None:
            result = []

        if len(result) >= end - start:
            return result[:end - start]

        for attempt in range(retries):
            try:
                base_url = self.base_url
                response = requests.get(
                    base_url,
                    params={
                        "limit": batch_size,
                        "offset": start + len(result)
                    }
                )
                response.raise_for_status()
                data = response.json().get(
                    self.api_key,
                    []
                )

                logging.debug(
                    f"Fetched data (sample): {data[:2]}"
                )

                if not data:
                    return result
                result.extend(data)

                if len(result) >= end - start:
                    return result[:end - start]
                time.sleep(random.uniform(
                    delay,
                    delay + 2
                ))
                return self.fetch_data(
                    start,
                    end,
                    batch_size,
                    retries,
                    delay,
                    result
                )

            except RequestException as conn_e:
                logging.error(
                    f"Error fetching data: {conn_e}"
                )
                time.sleep(delay * (2 ** attempt))
            return result[:end - start]
        else:
            raise PostgresSourceTableLoaderNetworkException(
                "Max retries reached. Skipping to next batch."
            )

    def process_mol_batch(self, molecules, keys):
        """
        Tmethod processes a batch of molecular data by extracting specified
        keys and returning the data in a structured format suitable for
        further database insertion.
        :param molecules: A list of molecular data, where each molecule is
               represented as a dictionary.
        :param keys: A list of keys to extract from each molecule's data.
               Keys can be specified as strings for direct extraction or
               as tuples for nested extraction.
        :return:
        """
        def extract_nested(data, key_path):
            keys = key_path.split(".")
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

        df = pd.DataFrame(filtered_data)

        return df

    def ensure_column_order_and_types(self, df):
        """
        The method ensures that a given DataFrame (df)
        adheres to a specified column order and data types
        as defined in self.table_columns.
        :param df: The Pandas DataFrame to be processed,
               containing columns that need to be ordered and
               typed according to self.table_columns.
        :return: A modified DataFrame.
        """
        for col, dtype in self.table_columns.items():
            if col not in df.columns:
                df[col] = None
            if dtype == "str":
                df[col] = df[col].astype("object")
        df = df[
            self.table_columns.keys()
        ]
        return df

# TODO: create a separate class for this type of common func ?
    def convert_bool_to_numeric(self, df):
        """
        The method converts boolean columns in a given Pandas DataFrame
        to numeric (float) values.
        :param df: The Pandas DataFrame containing boolean columns to be
               converted to numeric values.
        :return: A modified DataFrame.
        """
        bool_columns = [
            col for col, dtype in self.table_columns.items() if dtype == "bool"
        ]
        for col in bool_columns:
            df[col] = df[col].astype(float)

        return df

    def fetch_and_insert(self, start, end):
        """
        The method retrieves data from an external source in batches,
        processes it, and inserts it into a PostgreSQL database.
        :param start: The starting index of the data retrieval process.
        :param end: The ending index (exclusive) of the data retrieval process.
        :return:
        """
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
                        f"No data fetched for offset {offset} to"
                        f"{min(offset + self.batch_size, end)}"
                    )
                    continue

                all_mol_param = [molecule for molecule in data]

                mols_df = self.process_mol_batch(
                    all_mol_param,
                    self.keys_to_extract
                )

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

                pg_connector = PostgresConnector(
                    PG_DATABASE,
                    PG_HOST,
                    PG_USER,
                    PG_PASSWORD,
                    PG_PORT
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
        """
        The method facilitates concurrent data retrieval and insertion
        into a PostgreSQL database by spawning multiple processes, each
        handling a subset of the data.
        """
        processes = []
        rows_per_process = self.total_rows // self.processes_num

        for i in range(self.processes_num):
            start = i * rows_per_process
            end = start + rows_per_process if i != self.processes_num - 1 else self.total_rows
            p = Process(
                target=self.fetch_and_insert,
                args=(
                    start,
                    end
                )
            )
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

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
    PG_HOST
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

        try:
            response = requests.get(self.base_url)
            response.raise_for_status()  # Will raise an HTTPError for bad responses
            data = response.json()
            if "page_meta" in data and "total_count" in data["page_meta"]:
                total_rows_count = data["page_meta"]["total_count"]
                logging.info(f"Total row count for {self.base_url} is {total_rows_count}.")
                return total_rows_count
            else:
                raise PostgresSourceTableLoaderUnexpectedResponseStructureException(
                    f"Unexpected response structure: {data}"
                )

        except (HTTPError, ConnectionError, Timeout) as conn_err:
            raise PostgresSourceTableLoaderNetworkException(
                f"Network error occurred: {conn_err}."
            )

    def fetch_data(
            self,
            start,
            end,
            batch_size=5000,
            retries=5,
            delay=2,
            result=None
    ):

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
                print(f"Error fetching data: {conn_e}")
                time.sleep(delay * (2 ** attempt))
            return result[:end - start]
        else:
            raise PostgresSourceTableLoaderNetworkException(
                "Max retries reached. Skipping to next batch."
            )

    def process_mol_batch(self, molecules, keys):

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
        for col, dtype in self.table_columns.items():
            if col not in df.columns:
                df[col] = None
            if dtype == "str":
                df[col] = df[col].astype("object")
        df = df[self.table_columns.keys()]
        return df

    def insert_data(self, df):
        engine = create_engine(
            f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}/{PG_DATABASE}"
        )
        Session = sessionmaker(bind=engine)
        session = Session()

        try:

            data = df.to_dict(orient="records")

            session.bulk_insert_mappings(self.base_class, data)
            session.commit()

            logging.info(
                f"Data is loaded into DB. Number of inserted rows: {len(df)}"
            )
        except IntegrityError as e:
            session.rollback()

            # Identify and remove duplicates
            existing_chembl_ids = session.query(self.base_class.chembl_id).all()
            existing_chembl_ids_set = set([row[0] for row in existing_chembl_ids])
            filtered_data = [
                record for record in data if record["chembl_id"] not in existing_chembl_ids_set
            ]

            if filtered_data:
                try:
                    session.bulk_insert_mappings(
                        self.base_class,
                        filtered_data
                    )
                    session.commit()
                    print(
                        f"Data is loaded into DB after removing duplicates. \
                        Number of inserted rows: {len(filtered_data)}"
                    )
                except IntegrityError as e:
                    session.rollback()
                    print(f"IntegrityError occurred during reinsert: {e}")

            else:
                print("No data to insert after removing duplicates.")
        except Exception as e:
            session.rollback()
            print(f"Error inserting data: {e}")
        finally:
            session.close()

    def fetch_and_insert(self, start, end):
        logging.info(
            f"Process starting for range {start} to {end}"
        )
        engine = create_engine(
            f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}/{PG_DATABASE}"
        )
        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            for offset in range(start, end, self.batch_size):
                logging.info(
                    f"Fetching data from offset {offset} to {min(offset + self.batch_size, end)}"
                )
                data = self.fetch_data(
                    offset,
                    min(offset + self.batch_size, end),
                    self.batch_size
                )
                if not data:
                    logging.warning(
                        f"No data fetched for offset {offset} to {min(offset + self.batch_size, end)}"
                    )
                    continue
                all_mol_param = [molecule for molecule in data]
                mols_df = self.process_mol_batch(all_mol_param, self.keys_to_extract)
                mols_df.rename(
                    columns={"molecule_chembl_id": "chembl_id"},
                    inplace=True
                )

                # Ensure columns are ordered and typed correctly
                mols_df = self.ensure_column_order_and_types(mols_df)

                # Convert DataFrame to list of dictionaries
                data_to_insert = mols_df.to_dict(orient="records")

                # Perform bulk insert
                session.bulk_insert_mappings(self.base_class, data_to_insert)
                session.commit()

                logging.info(
                    f"Inserted {len(data_to_insert)} rows into DB. \
                    Total rows: {offset + len(data_to_insert)}"
                )

        except Exception as e:
            print(f"Error inserting data: {e}")
            session.rollback()
        finally:
            session.close()

        logging.info(
            f"Process finished for range {start} to {end}"
        )

    def parallel_fetch_and_insert(self):
        processes = []
        rows_per_process = self.total_rows // self.processes_num

        for i in range(self.processes_num):
            start = i * rows_per_process
            end = start + rows_per_process if i != self.processes_num - 1 else self.total_rows
            p = Process(target=self.fetch_and_insert,
                        args=(start, end))
            processes.append(p)
            p.start()

        for p in processes:
            p.join()

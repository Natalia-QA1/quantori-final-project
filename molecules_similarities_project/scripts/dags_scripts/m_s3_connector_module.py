import logging
import re
from abc import ABC
from io import BytesIO

import boto3
import chardet
import pandas as pd
import pyarrow.parquet as pq
from airflow.hooks.base import BaseHook
from botocore.exceptions import (
    NoCredentialsError,
    ClientError
)


class DataS3BaseException(Exception):
    pass


class DataS3ConnectionError(DataS3BaseException):
    pass


class DataS3ClientError(DataS3BaseException):
    pass


class DataS3DownloaderBadRowsError(DataS3BaseException):

    pass


class DataS3(ABC):

    def __init__(self, aws_conn_id="aws_s3_bucket"):

        aws_conn = BaseHook.get_connection(aws_conn_id)
        extra_config = aws_conn.extra_dejson

        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=aws_conn.login,
            aws_secret_access_key=aws_conn.password,
            region_name=extra_config.get("region_name"),
            aws_session_token=extra_config.get("aws_session_token")
        )


class S3DataLoader(DataS3):

    def upload_data(self, file_name, bucket, folder_name, object_name=None):

        if object_name is None:
            object_name = f"{folder_name}/{file_name}"
        try:
            self.s3_client.upload_file(
                file_name,
                bucket,
                object_name
            )
            logging.info(
                f"Uploaded {file_name} to {bucket}/{object_name}"
            )

        except NoCredentialsError:
            raise DataS3ConnectionError(
                "Credentials not available."
            )

        return True


class S3DataDownloader(DataS3):

    def __init__(self, bucket_name, folder, pattern):
        super().__init__()
        self.bucket_name = bucket_name
        self.folder = folder
        self.pattern = pattern
        self.files_names = self.check_for_files()

    def check_for_files(self):

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=self.folder
            )
        except ClientError as e:
            raise DataS3ClientError(
                f"Failed to list objects in bucket '{self.bucket_name}': {e}"
            )

        if "Contents" not in response:
            logging.warning(
                f"No contents found in bucket {self.bucket_name}"
                f"with prefix {self.folder}"
            )
            return []

        regex = re.compile(self.pattern)

        file_names = [item["Key"] for item in response.get("Contents", []) if
                      regex.search(item["Key"].replace(self.folder, ''))]

        logging.info(
            f"Found {len(file_names)} files matching pattern {self.pattern}"
            f"in bucket {self.bucket_name}/{self.folder}"
        )

        return file_names

# TODO: refactor code: create separate inherit classes for different files types
    def download_csv_data(self, encoding="utf-8"):

        dfs = []

        for file_name in self.files_names:
            try:
                obj = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=file_name
                )
                file_content = obj["Body"].read()
                detected_encoding = chardet.detect(file_content)["encoding"]
                logging.info(
                    f"Detected encoding for {file_name}: {detected_encoding}"
                )

                if detected_encoding is None:
                    logging.warning(
                        f"Failed to detect encoding for {file_name}"
                        f"using fallback encoding 'utf-8'"
                    )
                    detected_encoding = "utf-8"

                file_content_decoded = file_content.decode(
                    detected_encoding
                )

                valid_rows = []
                skipped_rows = []
                for line in file_content_decoded.splitlines():
                    try:
                        pd.read_csv(
                            BytesIO(line.encode(encoding)),
                            header=None
                        )
                        valid_rows.append(line)
                    except (UnicodeDecodeError, pd.errors.ParserError) as e:
                        logging.error(
                            f"Error parsing file {file_name}, line: {line}. Error: {e}"
                        )
                        skipped_rows.append(line)

                # Log skipped rows due to errors
                if skipped_rows:
                    error_file_name = f'{file_name}_parser_errors.log'
                    with open(error_file_name, "a") as error_log:
                        error_log.write(f"Error parsing file {file_name}")
                        error_log.write(f"Skipped rows:\n")
                        for line in skipped_rows:
                            error_log.write(f"{line}\n")


                file_obj = BytesIO('\n'.join(valid_rows).encode(encoding))
                df = pd.read_csv(
                    file_obj,
                    on_bad_lines="warn"
                )
                dfs.append(df)

            except ClientError as e:
                raise DataS3ClientError(
                    f"Failed to get object '{file_name}': {e}"
                )

        return dfs if dfs else []

    def download_parquet_data(self):

        dfs = []
        for file_name in self.files_names:
            try:
                obj = self.s3_client.get_object(
                    Bucket=self.bucket_name,
                    Key=file_name
                )
                parquet_file = BytesIO(obj["Body"].read())
                table = pq.read_table(parquet_file)
                df = table.to_pandas()
                dfs.append(df)

            except ClientError as e:
                raise DataS3ClientError(
                    f"Failed to get Parquet object {file_name}: {e}"
                )

        return dfs

    def combine_data(self, dfs, column_patterns):

        def standardize_column_name(column_name):
            for standard_name, patterns in column_patterns.items():
                for pattern in patterns:
                    if re.match(
                            pattern,
                            column_name.strip(),
                            re.IGNORECASE
                    ):

                        return standard_name.strip()

            return column_name.strip()

        standardized_dfs = []

        for df in dfs:

            standardized_columns = [standardize_column_name(col) for col in df.columns]
            df.columns = standardized_columns
            standardized_dfs.append(df)

        if not standardized_dfs:
            logging.warning(
                "No DataFrames to concatenate. The list of DataFrames is empty."
            )
            return pd.DataFrame()

        combined_df = pd.concat(
            standardized_dfs,
            ignore_index=True
        )

        return combined_df

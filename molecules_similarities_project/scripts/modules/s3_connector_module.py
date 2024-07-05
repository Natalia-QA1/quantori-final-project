import logging
import re
from abc import ABC
from io import BytesIO

import boto3
import chardet
import pyarrow.parquet as pq
import pandas as pd
from botocore.exceptions import (
    NoCredentialsError,
    ClientError
)

from molecules_similarities_project.config.config import (
    AWS_ACCESS_KEY,
    AWS_SECRET_KEY,
    AWS_SESSION_TOKEN,
    REGION_NAME
)

logging.basicConfig(level=logging.INFO)


class DataS3BaseException(Exception):
    pass


class DataS3ConnectionError(DataS3BaseException):
    pass


class DataS3ClientError(DataS3BaseException):
    pass


class DataS3DownloaderBadRowsError(DataS3BaseException):

    pass


class DataS3(ABC):
    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY,
            region_name=REGION_NAME,
            aws_session_token=AWS_SESSION_TOKEN
        )


class S3DataLoader(DataS3):
    def upload_data(self, file_name, bucket, folder_name, object_name=None):
        """
        The method uploads a file to an Amazon S3 bucket.
        It handles the construction of the object name within the specified S3 folder.
        :param file_name: The name of the file to be uploaded.
        :param bucket: The name of the S3 bucket where the file will be uploaded.
        :param folder_name: The name of the folder within the S3 bucket where the
               file will be stored.
        :param object_name: The S3 object name. If not provided, it defaults to
        {folder_name}/{file_name}.
        :return: bool: Returns True if the upload is successful.
        """
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
        """
        The method lists and filters files in a specified Amazon S3 bucket
        and folder based on a given regex pattern.
        :return: A list of files names.
        """
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
        """
        The method ownload CSV files from an Amazon S3 bucket, handle
        potential encoding issues, parse the CSV content, and return
        the data as a list of pandas DataFrames.
        :param encoding: The fallback encoding to use if encoding
               detection fails.
        :return: A list of pandas DataFrames containing the data from
        the CSV files. If no files are successfully parsed, an empty
        list is returned.
        """
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

                # Create a DataFrame from the valid rows
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
        """
        The method is designed to download Parquet files from an Amazon
        S3 bucket, convert them into pandas DataFrames, and return
        these DataFrames as a list.
        """
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
        """
        standardize column names across multiple pandas DataFrames
        based on specified patterns and then combine these DataFrames
        into a single DataFrame. This is useful when dealing with data
        from different sources where column names may vary but represent
        the same data.
        :param dfs: List of pandas DataFrames to be combined.
        :param column_patterns:
        :return: Dictionary where keys are standardized column names and
               values are lists of regex patterns that match various
               column name variations.
        """
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

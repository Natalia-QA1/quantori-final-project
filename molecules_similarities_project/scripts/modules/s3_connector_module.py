import logging
import re
from abc import ABC
from io import BytesIO

import boto3
import chardet
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
    def download_data(self, bucket_name, folder, pattern, valid_encoding="utf-8"):
        """
        The method retrieves and processes CSV files from a specified
        S3 bucket and folder. It filters files based on a regex pattern,
        detects their encoding, decodes their content, and reads them
        into Pandas DataFrames.
        :param bucket_name: The name of the S3 bucket from which to download files.
        :param folder: The folder (prefix) within the S3 bucket to search for files.
        :param pattern: A regex pattern to filter the files to be downloaded.
        :param valid_encoding: The encoding to use for decoding the files' content.
               Defaults to "utf-8".
        :return: A list of Pandas DataFrames, each representing the content of a
                successfully processed file. Returns an empty list if no files are
                found or processed.
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=folder
            )
        except ClientError as e:
            raise DataS3ClientError(
                f"Failed to list objects in bucket '{bucket_name}': {e}"
            )

        if 'Contents' not in response:
            logging.warning(
                f"No contents found in bucket '{bucket_name}' with prefix '{folder}'"
            )
            return []

        regex = re.compile(pattern)

        # Filter files based on the regex pattern
        file_names = [item['Key'] for item in response.get('Contents', []) if
                      regex.search(item['Key'].replace(folder, ''))]

        logging.info(
            f"Found {len(file_names)} files matching pattern '{pattern}' \
            in bucket '{bucket_name}/{folder}'"
        )

        # Initialize an empty list to store DataFrames
        dfs = []

        for file_name in file_names:
            try:
                obj = self.s3_client.get_object(
                    Bucket=bucket_name,
                    Key=file_name
                )
                file_content = obj["Body"].read()
                detected_encoding = chardet.detect(file_content)["encoding"]
                logging.info(
                    f"Detected encoding for '{file_name}': {detected_encoding}"
                )

                file_content_decoded = file_content.decode(
                    detected_encoding
                )

                # Read the CSV row by row, handle decode errors and ParserError
                valid_rows = []
                skipped_rows = []
                for line in file_content_decoded.splitlines():
                    try:
                        pd.read_csv(BytesIO(
                            line.encode(valid_encoding)),
                            header=None
                        )
                        valid_rows.append(line)
                    except (UnicodeDecodeError, pd.errors.ParserError) as e:
                        logging.error(
                            f"Error parsing file '{file_name}', line: {line}. Error: {e}"
                        )
                        skipped_rows.append(line)

                # Log skipped rows due to errors
                if skipped_rows:
                    error_file_name = f"{file_name}_parser_errors.log"
                    with open(error_file_name, "a") as error_log:
                        error_log.write(
                            f"Error parsing file '{file_name}':"
                        )
                        error_log.write(
                            f"Skipped rows:\n"
                        )
                        for line in skipped_rows:
                            error_log.write(
                                f"{line}\n"
                            )

                # Create a DataFrame from the valid rows
                file_obj = BytesIO('\n'.join(valid_rows).encode(valid_encoding))
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

    def combine_data(self, dfs, column_patterns):
        """
        The method standardizes the column names of multiple Pandas
        DataFrames based on provided patterns and then concatenates
        these DataFrames into a single DataFrame.
        :param dfs: A list of Pandas DataFrames to be standardized
               and combined returned by download_data method.
        :param column_patterns: A dictionary where the keys are the
               standardized column names and the values are lists of
               regex patterns that match the original column names to
               be standardized.
        :return: A single Pandas DataFrame resulting from the concatenation
                 of the standardized DataFrames. If the input list dfs is
                 empty, an empty DataFrame is returned.
        """
        def standardize_column_name(column_name):
            for standard_name, patterns in column_patterns.items():
                for pattern in patterns:
                    if re.match(
                            pattern,
                            column_name,
                            re.IGNORECASE
                    ):
                        return standard_name
            return column_name

        # Standardize column names for each DataFrame
        standardized_dfs = []
        for df in dfs:
            standardized_columns = [standardize_column_name(col) for col in df.columns]
            df.columns = standardized_columns
            standardized_dfs.append(df)

        # Check if there are any DataFrames to concatenate
        if not standardized_dfs:
            logging.warning(
                "No DataFrames to concatenate. \
                 The list of DataFrames is empty."
            )
            return pd.DataFrame()

        # Combine all DataFrames
        combined_df = pd.concat(
            standardized_dfs,
            ignore_index=True
        )

        return combined_df

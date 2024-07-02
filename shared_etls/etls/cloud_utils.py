import sys  # noqa
import logging  # noqa
from sness.cloud import s3_session
from botocore.exceptions import ClientError
from sness.datalake.metadata import Environment, Zone
from datetime import timedelta, datetime
from google.cloud import storage
from google.api_core.exceptions import NotFound
from sness.cloud import s3_session
import json  # noqa
import google.auth  # noqa
import pyspark.sql.types as st  # noqa
from pyspark.sql.functions import col, max


storage_client = storage.Client()
BUCKET_PREFIX = "prd-lake-work-"


class LoggerWriter:
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level

    def write(self, message):
        if message.strip():
            if not message.endswith("\n"):
                message += "\n"
            self.logger.log(self.level, message.strip())
            sys.__stdout__.write(message)

    def flush(self):
        pass


def build_logger_writer(namespace_name: str, dataset_name: str, timestamp: str):
    """
    Builds a logging configuration and redirects sys.stdout and sys.stderr to the configured logger.

    Parameters:
        :param namespace_name (str): The namespace name.
        :param dataset_name (str): The dataset name.
        :param timestamp (str): The timestamp to be included in the log file name.

    Returns:
        :param filename, storage_bucket_name, remote_log_file:
        Tuple[str, str, str]: A tuple containing the generated log file name,
          storage bucket name, and remote log file path.
    """

    filename = f"tycoon-{namespace_name}-{dataset_name}-{timestamp}.log"

    logging.basicConfig(
        filename=filename,
        level=logging.INFO,
        format="%(message)s",
    )

    sys.stdout = LoggerWriter(logging.getLogger(), logging.INFO)
    sys.stderr = LoggerWriter(logging.getLogger(), logging.ERROR)

    storage_bucket_name = f"{BUCKET_PREFIX}tycoon"
    remote_log_file = f"logs/{filename}"

    return filename, storage_bucket_name, remote_log_file


def update_logs(
    mensage: str, filename: str, storage_bucket_name: str, remote_log_file: str
):
    """update_logs
    function responsible for printing messages during the
    execution of the etl process and responsible for updating
    the file within the bucket with the most recent message.

    :param mensage: str: message to be printed and updated in the log file
    :param filename: str: name of the log file
    :param storage_bucket_name: str: name of the bucket where the log file is stored
    :param remote_log_file: str: path to the log file in the bucket

    :return: bool: True if the message was updated successfully, False otherwise
    """
    try:
        print(mensage)

        bucket = storage_client.bucket(storage_bucket_name)
        blob = bucket.blob(remote_log_file)
        blob.upload_from_filename(filename)

        return True
    except Exception as e:
        logging.error(f"Error sending log file to GCP: {e}")
        return False


def create_gcp_bucket(bucket_name, filename, storage_bucket_name, remote_log_file):
    """
    Creates a Google Cloud Platform (GCP) storage bucket in the specified region.

    :param bucket_name: str: The name of the bucket to be created.
    :param filename: str: The name of the local log file where logs will be recorded.
    :param storage_bucket_name: str: The name of the storage bucket where the log file
      will be stored.
    :param remote_log_file: str: The name of the log file in the storage bucket.

    :return: google.cloud.storage.bucket.Bucket: The created bucket object if successful.

    :raises ClientError: If there is an error creating the bucket.
    """

    try:
        mensage = f">>> [INFO] Creating bucket in GCP"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        client = storage.Client()
        bucket = client.create_bucket(bucket_name, location="us-east1")

        mensage = f">>> [INFO] Bucket {bucket.name} successfully created in the region {bucket.location}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
    except ClientError as e:
        mensage = f'>>> [WARN] Error when creating Bucket "{bucket_name} on GCP": {e}'
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        raise





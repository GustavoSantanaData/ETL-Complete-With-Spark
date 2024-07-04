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


def create_mgc_bucket(bucket_name, filename, storage_bucket_name, remote_log_file):
    """
    Creates a bucket at MGC.

    :param bucket_name: str: The name of the bucket to be created.
    :param filename: str: The name of the local log file where logs will be recorded.
    :param storage_bucket_name: str: The name of the storage bucket where the log file
      will be stored.
    :param remote_log_file: str: The name of the log file in the storage bucket.

    :return: None

    :raises ClientError: If there is an error creating the bucket.
    """

    s3_client = s3_session.boto3_session()
    mensage = f">>> [INFO] Creating bucket in MGC"
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    try:
        s3_client.create_bucket(Bucket=bucket_name)
        mensage = f">>> [INFO] The bucket was successfully created at MGC."
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
    except ClientError as e:
        mensage = f'>>> [WARN] Error when creating Bucket "{bucket_name} on MGC": {e}'
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        raise


def build_bucket_name(namespace_name: str, cloud_environment: str):
    """
    Builds and formats the name of the bucket that will be used when executing the ETL process.

    :param namespace_name: str: The name of the namespace.
    :param cloud_environment: str: The name of the cloud environment.

    :return: str: The formatted name of the bucket.
    """

    if cloud_environment == "mgc":
        bucket_name = f"{BUCKET_PREFIX}{namespace_name}"
        return bucket_name.replace("_", "-")
    else:
        return f"{BUCKET_PREFIX}{namespace_name}"


def clear_dataset_inside_bucket_gcp(
    namespace_name: str,
    cloud_environment: str,
    filename: str,
    storage_bucket_name: str,
    remote_log_file: str,
    dataset_name: str,
):
    """
    Clears all objects from the specified bucket and dataset in the cloud environment.

    :param namespace_name: str: The namespace name.
    :param cloud_environment: str: The cloud environment where the bucket exists.
    :param filename: str: The name of the log file.
    :param storage_bucket_name: str: The name of the storage bucket.
    :param remote_log_file: str: The path of the remote log file.
    :param dataset_name: str: The dataset.

    :raises ClientError: An error occurred while trying to clear the bucket.
    """

    bucket_name = build_bucket_name(namespace_name, cloud_environment)
    prefix = dataset_name

    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix=prefix)

        for blob in blobs:
            blob.delete()

        mensagem = f">>> [INFO] All objects inside the folder '{prefix}' were successfully deleted."
        update_logs(mensagem, filename, storage_bucket_name, remote_log_file)
    except:
        mensagem = f'>>> [WARN] Error deleting "{dataset_name}" from bucket'
        update_logs(mensagem, filename, storage_bucket_name, remote_log_file)


def clear_dataset_inside_bucket_mgc(
    namespace_name: str,
    cloud_environment: str,
    filename: str,
    storage_bucket_name: str,
    remote_log_file: str,
    dataset_name: str,
):
    """
    Clears all objects from the specified bucket and dataset in the cloud environment.

    :param namespace_name: str: The namespace name.
    :param cloud_environment: str: The cloud environment where the bucket exists.
    :param filename: str: The name of the log file.
    :param storage_bucket_name: str: The name of the storage bucket.
    :param remote_log_file: str: The path of the remote log file.
    :param dataset_name: str: The dataset.

    :raises ClientError: An error occurred while trying to clear the bucket.
    """

    bucket_name = build_bucket_name(namespace_name, cloud_environment)
    try:
        s3 = s3_session.boto3_session()
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=dataset_name)

        if "Contents" in response:
            for item in response["Contents"]:
                s3.delete_object(Bucket=bucket_name, Key=item["Key"])

        s3.delete_object(Bucket=bucket_name, Key=dataset_name)
        mensage = f'>>> [INFO] The file "{dataset_name}" was successfully deleted "{bucket_name}"'
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
    except ClientError as e:
        mensage = f'>>> [WARN] Error when deleting folder "{dataset_name}" from bucket "{bucket_name}": {e}'
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)


def bucket_exists(
    namespace_name: str,
    cloud_environment: str,
    filename: str,
    storage_bucket_name: str,
    remote_log_file: str,
    dataset_name: str,
):
    """
    Checks if a bucket exists in the specified cloud environment.

    :param namespace_name: str: The namespace name.
    :param cloud_environment: str: The cloud environment where the bucket exists.
    :param filename: str: The name of the log file.
    :param storage_bucket_name: str: The name of the storage bucket.
    :param remote_log_file: str: The path of the remote log file.
    :param dataset_name: str: The dataset name.

    :return: Tuple[Dict[str, Optional[bool]], str]: A tuple containing a dictionary with the status
      of the bucket existence in each cloud environment and the name of the bucket.
    """

    bucket_name = build_bucket_name(namespace_name, cloud_environment)
    status_cloud_exist = {"mgc": None, "gcp": None}

    if cloud_environment == "mgc":
        try:
            mensage = f">>> [INFO] Searching on mgc..."
            update_logs(mensage, filename, storage_bucket_name, remote_log_file)

            s3_client = s3_session.boto3_session()
            s3_client.head_bucket(Bucket=bucket_name)
            mensage = f'>>> [INFO] Bucket "{bucket_name}" found at mgc.'
            update_logs(mensage, filename, storage_bucket_name, remote_log_file)

            status_cloud_exist["mgc"] = True
        except ClientError as e:
            status_cloud_exist["mgc"] = False
            mensage = f">>> [WARN] The bucket wasnt found at mgc: {e}"
            update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    try:
        mensage = f">>> [INFO] Searching on GCP..."
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        client = storage.Client()
        bucket = client.bucket(bucket_name)
        bucket_exists = bucket.exists()

        if not bucket_exists:
            status_cloud_exist["gcp"] = False
        else:
            status_cloud_exist["gcp"] = True
            mensage = f'>>> [INFO] Bucket "{bucket_name}" found at GCP.'
            update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    except NotFound as e:
        mensage = f">>> [WARN] The bucket wasnt found at GCP: {e}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        status_cloud_exist["gcp"] = False

    return status_cloud_exist, bucket_name


def clear_current_bucket_or_create_a_new_bucket(
    bucket_already_exists,
    bucket_name,
    namespace_name,
    cloud_environment,
    filename,
    storage_bucket_name,
    remote_log_file,
    dataset_name,
):
    """
    Clears the current bucket if it exists or creates a new bucket in the specified cloud
      environment.

    :param bucket_already_exists: Dict[str, bool]: A dictionary indicating whether the bucket exists
      in each cloud environment.
    :param bucket_name: str: The name of the bucket.
    :param namespace_name: str: The namespace name.
    :param cloud_environment: str: The cloud environment where the bucket exists.
    :param filename: str: The name of the log file.
    :param storage_bucket_name: str: The name of the storage bucket.
    :param remote_log_file: str: The path of the remote log file.
    :param dataset_name: str: The dataset name.

    :return: None
    """

    if bucket_already_exists["mgc"] is True:
        mensage = f">>> [INFO] Bucket {bucket_name} already exists in MGC."
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        clear_dataset_inside_bucket_mgc(
            namespace_name,
            cloud_environment,
            filename,
            storage_bucket_name,
            remote_log_file,
            dataset_name,
        )
    else:
        mensage = f">>> [INFO] Bucket {bucket_name} not exists in MGC."
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        create_mgc_bucket(bucket_name, filename, storage_bucket_name, remote_log_file)

    if bucket_already_exists["gcp"] is True:
        mensage = f">>> [INFO] Bucket {bucket_name} already exists in GCP."
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        clear_dataset_inside_bucket_gcp(
            namespace_name,
            cloud_environment,
            filename,
            storage_bucket_name,
            remote_log_file,
            dataset_name,
        )
    else:
        mensage = f">>> [WARN] Bucket {bucket_name} not exists in GCP."
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        create_gcp_bucket(bucket_name, filename, storage_bucket_name, remote_log_file)


def _get_credentials_gcp():
    """
    Retrieves the default Google Cloud Platform (GCP) credentials.

    :return: The default GCP credentials.
    """

    credentials, _ = google.auth.default()
    return credentials


def get_file_content_gcp(
    bucket_name, object_key, filename, storage_bucket_name, remote_log_file
):
    """
    Retrieves the content of a file stored in a Google Cloud Storage bucket.

    :param bucket_name: str: The name of the GCP bucket.
    :param object_key: str: The key (path) of the file within the bucket.

    :return: Optional[Dict]: The content of the file as a dictionary, or None if an error occurs.
    """

    try:
        credentials = _get_credentials_gcp()
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(object_key)
        file_content = blob.download_as_string().decode("utf-8")
        return json.loads(file_content)
    except Exception as e:
        mensage = f">>> [WARN] Erro ao recuperar o conteúdo do arquivo: {e}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        return None


def get_file_content_mgc(
    bucket_name, object_key, filename, storage_bucket_name, remote_log_file
):
    """
    Retrieves the content of a file stored in a Minio Gateway for Ceph (MGC) bucket.

    :param bucket_name: str: The name of the MGC bucket.
    :param object_key: str: The key (path) of the file within the bucket.

    :return: Optional[Dict]: The content of the file as a dictionary, or None if an error occurs.
    """

    s3 = s3_session.boto3_session()

    try:
        response = s3.get_object(Bucket=bucket_name, Key=object_key)

        file_content = response["Body"].read().decode("utf-8")

        return json.loads(file_content)
    except Exception as e:
        mensage = f">>> [WARN] Erro ao recuperar o conteúdo do arquivo: {e}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        return None


def write_dataset_bukcet(
    df, DSMetadata, filename, storage_bucket_name, remote_log_file, ss
):
    """
    Writes a dataset to the work zone.

    :param df: DataFrame: The DataFrame containing the dataset.
    :param DSMetadata: DSMetadata: The metadata object containing information about the dataset.
    :param filename: str: The name of the file to be created in the storage bucket, including the file extension (e.g., 'data.csv').
    :param storage_bucket_name: str: The name of the cloud storage bucket where the dataset will be uploaded.
    :param remote_log_file: str: The path to a remote log file where operational details and metadata will be recorded.
    :param ss: SparkSession: initialize the spark session through snessSpark and
      assign it to the ss parameter

    :raises ValueError: If there are any problems while writing the dataset.
    """

    try:
        mensage = ">>> [INFO] writing dataset to work zone"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        ss.write.parquet(
            df,
            environment=Environment.PRODUCTION,
            zone=Zone.WORK,
            namespace=DSMetadata.namespace,
            dataset=f"{DSMetadata.dataset}",
            cloud_environment="gcp",
            mode="append",
        )

        mensage = ">>> [INFO] write dataset successfully"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    except Exception as err:
        mensage = f">>> [WARN] Problems while write dataset {err}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        raise ValueError(f">>> [WARN] Problems while write dataset {err}")


def merge_dataset_bucket(
    df_parquet,
    DSMetadata,
    cloud_environment,
    ss,
    filename,
    storage_bucket_name,
    remote_log_file,
):
    """
    Merges a Parquet dataset into a trusted work zone using the specified Spark session and
      dataset metadata.

    Parameters:
    :param df_parquet : DataFrame
        The Parquet DataFrame to be merged.
    :param DSMetadata : DSMetadataClass
        An object containing metadata information such as namespace and dataset details.
    :param cloud_environment : str
        The cloud environment in which the merge operation will be executed.
    :param ss : SparkSession
        The Spark session to be used for merging the dataset.
    :param filename : str
        The name of the file being processed, used for logging purposes.
    :param storage_bucket_name : str
        The name of the storage bucket where logs and datasets are stored.
    :param remote_log_file : str
        The remote log file to be updated with log messages.

    Returns:
    None
    """

    try:
        mensage = ">>> [INFO] merging dataset to trusted zone"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        ss.merge.parquet(
            df_parquet,
            environment=Environment.PRODUCTION,
            zone=Zone.TRUSTED,
            namespace=DSMetadata.namespace,
            dataset=DSMetadata.dataset,
            merge_keys="lake.sk = df.sk",
            delta_retention_hours=72,
            cloud_environment=cloud_environment,
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        mensage = ">>> [INFO] end of merge process"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    except Exception as err:
        mensage = f">>> [WARN] Problems while merge dataset {err}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        raise ValueError(f">>> [WARN] Problems while write dataset {err}")

import sys  # noqa
from sness.datalake.metadata import Environment, Zone, DatasetMetadata
from datetime import timedelta, datetime
import requests
import re  # noqa
import json  # noqa
import pyspark.sql.types as st  # noqa
import pyspark.sql.functions as sf


from cloud_utils import update_logs


def update_status(id_request: str, value: str):
    """
    Updates the status of a request with the specified ID.

    Parameters:
        :param id_request (str): The ID of the request to update.
        :param value (str): The new value for the status.

    Raises:
        requests.RequestException: An error occurred while trying to make the HTTP POST request.
    """

    data = {"id_request": id_request, "process": "resync_process", "value": value}
    requests.post("https://tycoon-api-hml-external.mglu.io/v1/update/status/", json=data)


def get_database_name_inside_string_conection(uri, namespace_name):
    """
    get database name inside string conection

    Args:
        uri (str): string de conexÃ£o
        namespace_name (str): namespace

    Returns:
        string: o nome do banco de dados
    """

    match = re.search(r"database=(\w+)", uri)
    if match:
        database = match.group(1)
    else:
        database = namespace_name
    return database


def get_schema_from_witcher(
    namespace_name, dataset, cloud, filename, storage_bucket_name, remote_log_file
):
    """
    Retrieves the schema of a dataset from the Witcher platform.

    :param namespace_name: str: The namespace name.
    :param dataset: str: The dataset name.
    :param cloud: str: The cloud environment where the schema file is stored.
    :param filename: str: The name of the log file.
    :param storage_bucket_name: str: The name of the storage bucket.
    :param remote_log_file: str: The path of the remote log file.

    :return: Optional[Dict]: The schema of the dataset as a dictionary.
    """
    from cloud_utils import get_file_content_mgc, get_file_content_gcp

    try:

        if cloud == "mgc":
            mensage = f">>> [INFO] Reading data schema in witcher mgc"
            update_logs(mensage, filename, storage_bucket_name, remote_log_file)

            bucket_name = "prd-dags"
            object_key = f"witcher/schemas_json/{namespace_name}_{dataset}.json"

            file_content = get_file_content_mgc(
                bucket_name, object_key, filename, storage_bucket_name, remote_log_file
            )
        else:
            mensage = f">>> [INFO] Reading data schema in witcher GCP"
            update_logs(mensage, filename, storage_bucket_name, remote_log_file)

            bucket_name = "us-east1-prd-witcher-d6b72fab-bucket"
            object_key = f"dags/witcher/schemas_json/{namespace_name}_{dataset}.json"

            file_content = get_file_content_gcp(
                bucket_name, object_key, filename, storage_bucket_name, remote_log_file
            )
    except Exception as e:
        mensage = f">>> [WARN] Error capturing schema in witcher_files {e}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    return file_content


def get_schema_in_datalake_trusted_from_dataset(
    namespace_name, dataset, cloud, filename, storage_bucket_name, remote_log_file, ss
):
    """
    Retrieves the schema of a dataset from the Trusted zone of the Datalake.

    :param namespace_name: str: The namespace name.
    :param dataset: str: The dataset name.
    :param cloud: str: The cloud environment where the dataset resides.
    :param filename: str: The name of the log file.
    :param storage_bucket_name: str: The name of the storage bucket.
    :param remote_log_file: str: The path of the remote log file.
    param ss: SparkSession: An instance of SparkSession, used for data processing. using snessspark

    :return: custom_schema st.StructType: The schema of the dataset as a StructType.
    """

    mensage = f">>> [INFO] Reading data schema from Trusted"
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    try:
        DSMetadata = DatasetMetadata(namespace=namespace_name, dataset=dataset)
        df_read = ss.read.parquet(
            environment=Environment.PRODUCTION,
            zone=Zone.TRUSTED,
            namespace=DSMetadata.namespace,
            dataset=f"{DSMetadata.dataset}",
            cloud_environment=cloud,
        )

        schema = df_read.schema
        schema_json = schema.json()

        custom_schema_str = f"""
        {schema_json}
        """

        mensage = f">>> [INFO] Captured data schema"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        custom_schema = st.StructType.fromJson(json.loads(custom_schema_str))
    except Exception as e:
        mensage = f">>> [WARN] Error capturing schema in datalake {e}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    return custom_schema


def get_schema_to_mongodb_run(
    namespace_name, dataset, cloud, filename, storage_bucket_name, remote_log_file, ss
):
    """
    Retrieves the schema of a dataset to be used in MongoDB ingestion.

    :param namespace_name: str: The namespace name.
    :param dataset: str: The dataset name.
    :param cloud: str: The cloud environment where the dataset resides.
    :param filename: str: The name of the log file.
    :param storage_bucket_name: str: The name of the storage bucket.
    :param remote_log_file: str: The path of the remote log file.
    param ss: SparkSession: An instance of SparkSession, used for data processing. using snessspark

    :return: custom_schema [st.StructType]: The schema of the dataset as a StructType,
             or None if an error occurs.
    """

    file_content = get_schema_from_witcher(
        namespace_name, dataset, cloud, filename, storage_bucket_name, remote_log_file
    )

    if file_content is not None:
        custom_schema = st.StructType.fromJson(file_content)
        mensage = f">>> [INFO] Captured data schema"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        mensage = f">>> [INFO] {custom_schema}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
    else:
        custom_schema = get_schema_in_datalake_trusted_from_dataset(
            namespace_name,
            dataset,
            cloud,
            filename,
            storage_bucket_name,
            remote_log_file,
            ss,
        )

    return custom_schema


def quantity_level(quantity, start_date, end_date):
    """
    Determines the interval level based on the quantity and the duration between
    start_date and end_date.

    :param quantity: int: The quantity value.
    :param start_date: datetime.date: The start date.
    :param end_date: datetime.date: The end date.

    :return: intervalo int: The interval level.
    """

    quantity = quantity
    start_calc = start_date
    end_calc = end_date
    diff = (end_calc - start_calc).days
    intervalo = 1

    if quantity < 100000:
        intervalo = 15

    elif quantity < 500000:
        intervalo = 10
    elif quantity < 1000000:
        intervalo = 5
    elif quantity < 10000000:
        intervalo = 3
    elif quantity < 20000000:
        intervalo = 2
    else:
        intervalo = 2

    if diff == 1:
        intervalo = 1

    if diff < 15:
        intervalo = 2

    return intervalo


def quantity_level_int(quantity):
    """
    Defines the query interval and quantity level based on data quantity.

    :param quantity: int: The amount of data.

    :return: intervalo int: The defined interval for queries.
    """

    quantity = quantity
    intervalo = 1000

    if quantity < 100000:
        intervalo = 50000
    if quantity < 50000:
        intervalo = 25000
    if quantity < 25000:
        intervalo = 5000
    if quantity < 5000:
        intervalo = 1000
    else:
        intervalo = 100000

    return intervalo


def get_sega_driver(sega_url: str):
    """
    Extracts the database driver name from the given Sega URL.

    :param sega_url: str: The Sega URL containing information about the database driver.

    :return: sega_driver (str): The name of the database driver.
    """

    if "mongodb" in sega_url:
        sega_driver = "mongodb"
    else:
        sega_driver = sega_url.split("/")[0].split(":")[1]

    return sega_driver


def query_definition(dataset, partition_column, start, end):
    """
    Defines a query based on the provided parameters.

    :param dataset: str: The name of the dataset.
    :param partition_column: str: The name of the partition column.
    :param start: str: The start date for the query.
    :param end: str: The end date for the query.

    :return: query (str): The SQL query definition.
    """

    query = (
        f"(select * from {dataset} where {partition_column} >= '{start}'"
        f"and {partition_column} < '{end}') subs"
    )

    return query


def calculate_allintervals_todefinequery(
    start, end, days_distance_interval_query, type_of_process
):
    """
    Calculate date intervals between the start and end dates using the given interval of days and
    save all intervals inside array intervals.

    Parameters:
        :param start: datetime: The start date of the intervals.
        :param end: datetime: The end date of the intervals.
        :param days_distance_interval_query: int: The number of days per interval.
        :param type_of_process: str: The type of process. Either "date" or "int".

    Returns:
        :param interval: List[Tuple[Union[str, int], Union[str, int]]]: List of date intervals,
         where each tuple contains a start and end date or a start and end number.
    """

    if type_of_process == "date":
        intervals = []
        current_start_date = start
        while current_start_date < end:
            current_end_date = min(
                current_start_date + timedelta(days=days_distance_interval_query - 1),
                end,
            )
            intervals.append((current_start_date, current_end_date))
            current_start_date = current_end_date + timedelta(days=1)

        return intervals

    if type_of_process == "int":
        intervals = []
        current_start_num = start
        while current_start_num < end:
            current_end_num = min(
                current_start_num + days_distance_interval_query - 1, end
            )
            intervals.append((current_start_num, current_end_num))
            current_start_num = current_end_num + 1
        return intervals


def define_start_query_mongodb(
    start,
    partition_column,
    dataset_name,
    sega_url,
    filename,
    storage_bucket_name,
    remote_log_file,
    id_request,
    custom_schema,
    database,
    ss,
    type_of_load,
):
    """
    Defines the start date query for a given partition column and dataset name.
    If a start date string is provided, it converts it into a datetime object.
    If no start date string is provided, it retrieves the minimum value of the
    partition column from the database.

    Args:
        :param start (str or None): A string representing the start date in the format "YYYY-MM-DD".
        :param partition_column (str): The name of the partition column.
        :param dataset_name (str): The name of the dataset.
        :param sega_url (str): The URL or connection string for the database.
        :param filename (str): The name of the file.
        :param storage_bucket_name (str): The name of the storage bucket.
        :param remote_log_file (str): The path to the remote log file.
        :param id_request (str): The ID of the request.
        :param custom_schema (str, optional): The custom schema for the dataset. Defaults to None.
        :param database (str, optional): The name of the database. Defaults to None.
        :param ss: SparkSession: initialize the spark session through snessSpark and
      assign it to the ss parameter
        :param type_of_load (str, optional): The type of load. Defaults to None.

    Returns:
        :return: start_int_query datetime.datetime: The start date query as a datetime object.
    """
    from load_utils import get_database_minimumvalue_mongodb

    if type_of_load == "date":
        if start:
            start_date_query = datetime.strptime(start, "%Y-%m-%d")
        else:
            start_date_query = get_database_minimumvalue_mongodb(
                partition_column,
                dataset_name,
                sega_url,
                custom_schema,
                database,
                filename,
                storage_bucket_name,
                remote_log_file,
                id_request,
                ss,
            )

        return start_date_query

    if type_of_load == "int":
        if start:
            start_int_query = int(start)
        else:
            start_int_query = get_database_minimumvalue_mongodb(
                partition_column,
                dataset_name,
                sega_url,
                custom_schema,
                database,
                filename,
                storage_bucket_name,
                remote_log_file,
                id_request,
                ss,
            )

            start_int_query = int(start_int_query)

        return start_int_query


def define_end_query_mongodb(
    end,
    partition_column,
    dataset_name,
    sega_url,
    database,
    namespace_name,
    cloud_environment,
    filename,
    storage_bucket_name,
    remote_log_file,
    ss,
    type_of_load,
):
    """
    Define the end date query based on the provided end date string. If an end date string is
    provided, it converts it into a datetime object. If no end date string is provided, it defaults
    to the current date.

    Args:
        :param end (str or None): A string representing the end date in the format "YYYY-MM-DD".
        :param partition_column (str): The name of the partition column.
        :param dataset_name (str): The name of the dataset.
        :param sega_url (str): The URL or connection string for the database.
        :param database (str): The name of the database.
        :param namespace_name (str): The namespace name.
        :param cloud_environment (str): The cloud environment.
        :param filename (str): The name of the file.
        :param storage_bucket_name (str): The name of the storage bucket.
        :param remote_log_file (str): The path to the remote log file.
        :param ss: SparkSession: initialize the spark session through snessSpark and
      assign it to the ss parameter
        :param type_of_load (str): The type of load.

    Returns:
        :return: end_date_query datetime or int: The end date query as a datetime object or an
          integer value.
    """
    from load_utils import calc_max_partition_value_mongodb

    if type_of_load == "date":
        if end:
            end_date_query = datetime.strptime(end, "%Y-%m-%d")
        else:
            end_date_str = str(datetime.now().strftime("%Y-%m-%d"))
            end_date_query = datetime.strptime(end_date_str, "%Y-%m-%d")

        return end_date_query

    if type_of_load == "int":
        if end:
            end_date_query = int(end)
            end_date_query = end_date_query
        else:
            end_date_query = calc_max_partition_value_mongodb(
                partition_column,
                sega_url,
                database,
                namespace_name,
                dataset_name,
                cloud_environment,
                filename,
                storage_bucket_name,
                remote_log_file,
                ss,
            )

        return end_date_query


def define_start_query(
    start,
    partition_column,
    dataset_name,
    sega_user,
    sega_pass,
    sega_url,
    filename,
    storage_bucket_name,
    remote_log_file,
    id_request,
    ss,
    type_of_load,
):
    """
    Define the start date query for a given partition column and dataset name. If a start date
    string is provided, it converts it into a datetime object. If no start date string is provided,
    it retrieves the minimum value of the partition column from the database.

    Args:
        :param start (str or None): A string representing the start date in the format "YYYY-MM-DD".
        :param partition_column (str): The name of the partition column.
        :param dataset_name (str): The name of the dataset.
        :param sega_user (str): The username for accessing the database.
        :param sega_pass (str): The password for accessing the database.
        :param sega_url (str): The URL or connection string for the database.
        :param filename (str): The name of the file.
        :param storage_bucket_name (str): The name of the storage bucket.
        :param remote_log_file (str): The path to the remote log file.
        :param id_request (str): The ID of the request.
        :param ss: SparkSession: initialize the spark session through snessSpark and
      assign it to the ss parameter
        :param type_of_load (str): The type of load.

    Returns:
        :return: start_int_query datetime or int: The start date query as a datetime object
          or an integer value.
    """
    from load_utils import get_database_minimumvalue

    if type_of_load == "date":
        if start:
            start_date_query = datetime.strptime(start, "%Y-%m-%d")
        else:
            start_date_query = get_database_minimumvalue(
                partition_column,
                dataset_name,
                sega_user,
                sega_pass,
                sega_url,
                filename,
                storage_bucket_name,
                remote_log_file,
                id_request,
                ss,
            )

        return start_date_query

    if type_of_load == "int":
        if start:
            start_int_query = int(start)
        else:
            start_int_query = get_database_minimumvalue(
                partition_column,
                dataset_name,
                sega_user,
                sega_pass,
                sega_url,
                filename,
                storage_bucket_name,
                remote_log_file,
                id_request,
                ss,
            )

            start_int_query = int(start_int_query)

        return start_int_query


def define_end_query(
    end,
    partition_column,
    dataset_name,
    sega_url,
    sega_user,
    sega_pass,
    ss,
    filename,
    storage_bucket_name,
    remote_log_file,
    type_of_load,
):
    """
    Define the end date query based on the provided end date string. If an end date string is
    provided, it converts it into a datetime object. If no end date string is provided,
    it defaults to the current date.

    Args:
        :param end (str or None): A string representing the end date in the format "YYYY-MM-DD".
        :param partition_column (str): The name of the partition column.
        :param dataset_name (str): The name of the dataset.
        :param sega_url (str): The URL or connection string for the database.
        :param sega_user (str): The username for accessing the database.
        :param sega_pass (str): The password for accessing the database.
        :param ss: SparkSession: initialize the spark session through snessSpark and
      assign it to the ss parameter
        :param type_of_load (str): The type of load.

    Returns:
        :return: end_int datetime or int: The end date query as a datetime
          object or an integer value.
    """
    from load_utils import calc_max_partition_value

    if type_of_load == "date":
        if end:
            end_date_query = datetime.strptime(end, "%Y-%m-%d")
        else:
            end_date_str = str(datetime.now().strftime("%Y-%m-%d"))
            end_date_query = datetime.strptime(end_date_str, "%Y-%m-%d")

        return end_date_query

    if type_of_load == "int":
        if end:
            end_int = int(end)
            end_int = end_int
        else:
            end_int = calc_max_partition_value(
                partition_column,
                dataset_name,
                sega_url,
                sega_user,
                sega_pass,
                ss,
                filename,
                storage_bucket_name,
                remote_log_file,
            )

        return end_int


def sets_ssl_false_in_the_connection_string(sega_driver, sega_url):
    """
    Appends 'useSSL=false' to the connection string URL if the driver is MySQL.

    This function checks if the given `sega_driver` is "mysql". If it is, it modifies
    the `sega_url` to include the parameter `useSSL=false`. The function handles
    the cases where the URL already contains other parameters by appending with
    either '&' or '?' as appropriate.

    Args:
        :param: sega_driver (str): The database driver name. This function specifically checks
            for "mysql".
        :param sega_url (str): The connection string URL to which the 'useSSL=false' parameter
            will be appended if necessary.

    Returns:
        :param sega_url str: The modified connection string URL with 'useSSL=false'
          appended if the driver is "mysql".
    """

    if sega_driver == "mysql":

        if "?" in sega_url:
            sega_url += "&useSSL=false"
        else:
            sega_url += "?useSSL=false"

    return sega_url


def create_surrogate_key(
    ids, df, filename, storage_bucket_name, remote_log_file, enableHyphenSeparator=False
):
    """
    Creates a surrogate key for a DataFrame by concatenating specified ID columns and applying an MD5 hash.

    Parameters:
    :param ids : list
        A list of column names to be concatenated to form the surrogate key.
    :param df : DataFrame
        The DataFrame to which the surrogate key will be added.
    :param filename : str
        The name of the file being processed, used for logging purposes.
    :param storage_bucket_name : str
        The name of the storage bucket where logs and datasets are stored.
    :param remote_log_file : str
        The remote log file to be updated with log messages.

    Returns:
    DataFrame
        The DataFrame with the added surrogate key column.
    """

    mensage = f">>> [INFO] creating sk key"
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    if type(ids) == str:
        ids = [ids]

    if enableHyphenSeparator:
        df = (
            df.withColumn("concatenatedString", sf.concat_ws("-", *ids))
            .withColumn(
                "sk", sf.md5(sf.col("concatenatedString").cast(st.StringType()))
            )
            .drop("concatenatedString")
        )

    else:
        df = df.withColumn("sk", sf.concat(*ids)).withColumn(
            "sk", sf.md5(sf.col("sk").cast(st.StringType()))
        )
    return df


def add_timestamp_into_dataframe(df, filename, storage_bucket_name, remote_log_file):
    """
    :param df: Dataframe that will receive a current timestamp column
    :return: A dataframe with a column named 'timsetamp_kafka', used to
    merge and dedup correctly payloads and historical charge
    """

    mensage = ">>> [INFO] Adding column 'timestamp_kafka' used for Geralt to dedup"
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    df = df.withColumn(
        "timestamp_kafka",
        sf.lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).cast(st.TimestampType()),
    )

    return df


def quality_check_dataformat(df, filename, storage_bucket_name, remote_log_file):
    """
    :param df: dataframe to check value of timestamp and date columns
    :return: dataframe without dates with year 0009 or less
    """

    mensage = ">>> [INFO] Replacing dates with year 0009 or less with NULL"
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    try:
        coldates = [
            cols
            for cols in df.columns
            if df.select(cols).dtypes[0][1] in ["timestamp", "date"]
        ]
        for colname in coldates:
            try:
                df = df.withColumn(
                    colname,
                    sf.when(sf.year(colname) >= 10, sf.col(colname)).otherwise(None),
                )
            except Exception as ms:
                mensage = ">>> [WARN] There was a problem while handling invalid"
                update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    except Exception as msg:
        mensage = f">>> [WARN] There was a problem while handling invalid dates: {msg}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    return df


def extract_and_transform_data_from_the_raw_zone(
    DSMetadata,
    ss,
    sk_key,
    filename,
    storage_bucket_name,
    remote_log_file,
    enableHyphenSeparator,
):
    """
    Extracts data from the raw zone, transforms it by adding a surrogate
    key and a timestamp, performs a quality check, and removes duplicate entries.

    Parameters:
    DSMetadata : DSMetadataClass
        An object containing metadata information such as namespace and dataset details.
    ss : SparkSession
        The Spark session to be used for reading and transforming the data.
    sk_key : list
        A list of column names to be used for generating the surrogate key.
    filename : str
        The name of the file being processed, used for logging purposes.
    storage_bucket_name : str
        The name of the storage bucket where logs and datasets are stored.
    remote_log_file : str
        The remote log file to be updated with log messages.

    Returns:
    DataFrame
        The transformed DataFrame with a surrogate key, timestamp, and duplicates removed.

    df_with_timestamp = add_timestamp_into_dataframe(
        df_parquet_surrogate_key, filename, storage_bucket_name, remote_log_file
    )

    df_parquet_dataform_check = quality_check_dataformat(
        df_with_timestamp, filename, storage_bucket_name, remote_log_file
    )

    df_parquet = df_parquet_dataform_check.dropDuplicates(["sk"])

    return df_parquet

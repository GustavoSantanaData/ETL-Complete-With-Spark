import sys  # noqa
from sness.datalake.metadata import DatasetMetadata
from datetime import timedelta, datetime
from pyspark.sql.functions import col, max

# from utils import query_definition, update_status, get_schema_to_mongodb_run
from utils import update_status
from cloud_utils import update_logs


def read_from_database(
        sega_user,
        sega_pass,
        connection_url,
        query,
        ss,
        filename,
        storage_bucket_name,
        remote_log_file
        ):
    """
    Reads data from a database using JDBC connection.

    :param sega_user: str: The username for database authentication.
    :param sega_pass: str: The password for database authentication.
    :param connection_url: str: The connection URL for the database.
    :param query: str: The SQL query to retrieve data.
    :param ss: SparkSession: initialize the spark session through snessSpark and
      assign it to the ss parameter

    :return: DataFrame: The DataFrame containing the queried data.
    """

    mensage = (f">>> [INFO] Starting Load from database")
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    df = (
        ss.spark.read.format("jdbc")
        .option("user", sega_user)
        .option("password", sega_pass)
        .option("url", connection_url)
        .option("dbTable", query)
        .load()
    )

    return df


def read_from_database_mongodb(
    custom_schema,
    database,
    dataset,
    start,
    end,
    partition_column,
    uri,
    ss,
    filename,
    storage_bucket_name,
    remote_log_file
):
    """
    Reads data from a MongoDB database.

    :param custom_schema: StructType: The schema of the dataset.
    :param database: str: The name of the MongoDB database.
    :param dataset: str: The name of the dataset (collection).
    :param start: str: The start date for the query.
    :param end: str: The end date for the query.
    :param partition_column: str: The name of the partition column.
    :param uri: str: The MongoDB connection URI.
    :param ss: SparkSession: initialize the spark session through snessSpark and
      assign it to the ss parameter

    :return: DataFrame: The DataFrame containing the queried data.
    """

    mensage = (f">>> [INFO] Starting Load from mongo database")
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)


    if custom_schema:
        df = (
            ss.spark.read.format("mongo")
            .option("uri", uri)
            .option("spark.mongodb.input.database", database)
            .option("user", "defaultDbUser")
            .option("spark.mongodb.input.collection", dataset)
            .load(schema=custom_schema, inferSchema=False)
            .filter(f"{partition_column} between '{start}' and '{end}'")
        )
    else:
        df = (
            ss.spark.read.format("mongo")
            .option("uri", uri)
            .option("spark.mongodb.input.database", database)
            .option("user", "defaultDbUser")
            .option("spark.mongodb.input.collection", dataset)
            .load()
            .filter(f"{partition_column} between '{start}' and '{end}'")
        )

    return df


def run_database_load_mongodb(
    namespace,
    dataset,
    uri,
    start,
    end,
    partition_column,
    database,
    custom_schema,
    filename,
    storage_bucket_name,
    ss,
    remote_log_file,
):
    """
    Runs the database load process for MongoDB.

    :param namespace: str: The namespace.
    :param dataset: str: The dataset name.
    :param uri: str: The MongoDB URI.
    :param start: str: The start date for the query.
    :param end: str: The end date for the query.
    :param partition_column: str: The name of the partition column.
    :param database: str: The name of the MongoDB database.
    :param custom_schema: StructType: The custom schema for the dataset.
    :param filename: str: The name of the log file.
    :param storage_bucket_name: str: The name of the storage bucket.
    :param ss: SparkSession: initialize the spark session through snessSpark and
      assign it to the ss parameter
    :param remote_log_file: str: The path of the remote log file.

    :return: None
    """
    from cloud_utils import write_dataset_bukcet

    DSMetadata = DatasetMetadata(namespace=namespace, dataset=dataset)
    try:
        df = read_from_database_mongodb(
            custom_schema,
            database,
            dataset,
            start,
            end,
            partition_column,
            uri,
            ss,
            filename,
            storage_bucket_name,
            remote_log_file
        )

        mensage = ">>> [INFO] Read from database successfully"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    except Exception as e:
        mensage = f">>> [WARN] Error capturing schema in datalake {e}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    write_dataset_bukcet(
        df, DSMetadata, filename, storage_bucket_name, remote_log_file, ss
    )


def run_database_load(
    namespace,
    dataset,
    connection_url,
    start,
    end,
    partition_column,
    sega_user,
    sega_pass,
    filename,
    storage_bucket_name,
    remote_log_file,
    ss,
    type_of_process,
):
    """
    Perform an load of data from a JDBC source to a Parquet file in the datalake.
    This function must execute the process for each of the intervals

    Parameters:
        :param namespace: str: The name of the namespace (e.g., schema or collection) from
        which to load data.
    :param dataset: str: The name of the dataset to be loaded.
    :param connection_url: str: The URL for connecting to the database.
    :param start: str: The start date for loading data, formatted as 'YYYY-MM-DD'.
    :param end: str: The end date for loading data, formatted as 'YYYY-MM-DD'.
    :param partition_column: str: The name of the column used for partitioning the data.
    :param sega_user: str: The username for authenticating with the database.
    :param sega_pass: str: The password for authenticating with the database.
    :param filename: str: The name of the file to be created in the storage bucket, including the
      file extension (e.g., 'data.csv').
    :param storage_bucket_name: str: The name of the cloud storage bucket where the dataset will
      be uploaded.
    :param remote_log_file: str: The path to a remote log file where operational details and
    metadata will be recorded.
    :param ss: SparkSession: An instance of SparkSession, used for data processing and interactions
      with cloud storage.
    :param type_of_load (str): receives and identifies whether the process type is date or int


    Returns:
        :return: None
    """
    from utils import query_definition
    from cloud_utils import write_dataset_bukcet

    DSMetadata = DatasetMetadata(namespace=namespace, dataset=dataset)

    try:
        if type_of_process == "date" or type_of_process == "int":

            query = query_definition(dataset, partition_column, start, end)

            df = read_from_database(
                sega_user,
                sega_pass,
                connection_url,
                query,
                ss,
                filename,
                storage_bucket_name,
                remote_log_file
                )

            mensage = ">>> [INFO] Read from database successfully"
            update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    except Exception as err:
        raise ValueError(f">>> [WARN] Problems while configure dataframe {err}")

    write_dataset_bukcet(
        df, DSMetadata, filename, storage_bucket_name, remote_log_file, ss
    )


def get_database_minimumvalue_mongodb(
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
):
    """
    Retrieves the minimum value of a partition column from a MongoDB collection.

    Parameters:
        :param partition_column: str: The name of the partition column.
        :param dataset_name: str: The name of the dataset.
        :param sega_url: str: The Sega URL for the MongoDB connection.
        :param custom_schema: Optional[StructType]: The custom schema for the dataset.
        :param database: str: The name of the MongoDB database.
        :param filename: str: The name of the log file.
        :param storage_bucket_name: str: The name of the storage bucket.
        :param remote_log_file: str: The path of the remote log file.
        :param id_request: str: The ID of the request.
        :param ss: SparkSession: An instance of SparkSession, used for data processing and interactions
      with cloud storage.

    Returns:
        :return: result Any: The minimum value of the partition column.
    """

    mensage = f">>> [INFO] Starting full load start capture"
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    try:
        mensage = f">>> [INFO] Get min partition_column value {partition_column}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        df = (
            ss.spark.read.format("mongo")
            .option("uri", sega_url)
            .option("spark.mongodb.input.database", database)
            .option("user", "defaultDbUser")
            .option("spark.mongodb.input.collection", dataset_name)
            .load(schema=custom_schema, inferSchema=False)
            .agg({partition_column: "min"})
        )

    except Exception as e:
        mensage = f">>> [WARN] Capture failed...{str(e)}"
        update_status(id_request, "failed")
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        sys.exit(1)

    result = df.collect()[0][0]
    mensage = f">>> [INFO] Charging will start from {result} with type {type(result)}"
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    return result


def get_database_minimumvalue(
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
):
    """
    Retrieves the minimum value of a partition column from a database table.

    Parameters:
        :param partition_column: str: The name of the partition column.
        :param dataset_name: str: The name of the dataset.
        :param sega_user: str: The username for the database connection.
        :param sega_pass: str: The password for the database connection.
        :param sega_url: str: The JDBC URL for the database connection.
        :param filename: str: The name of the log file.
        :param storage_bucket_name: str: The name of the storage bucket.
        :param remote_log_file: str: The path of the remote log file.
        :param id_request: str: The ID of the request.
        :param ss: SparkSession: An instance of SparkSession, used for data processing and
        interactions
      with cloud storage.

    Returns:
        :return: result Any: The minimum value of the partition column from the database.
    """

    try:
        mensage = f">>> [INFO] Get min partition_column value {partition_column}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        query = f"(select min({partition_column}) from {dataset_name}) subs"
        df = (
            ss.spark.read.format("jdbc")
            .option("user", sega_user)
            .option("password", sega_pass)
            .option("url", sega_url)
            .option("dbTable", query)
            .load()
        )
    except Exception as e:
        mensage = f">>> [WARN] Capture failed...{str(e)}"
        update_status(id_request, "failed")
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        sys.exit(1)

    result = df.collect()[0][0]
    mensage = f">>> [INFO] Charging will start from {result} with type {type(result)}"
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    return result


def calc_max_partition_value_mongodb(
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
):
    """
    Calc max partition value from base origem

    Args:
        :param partition_column: str: The name of the column used for partitioning the dataset.
    :param sega_url: str: The URL of the MongoDB server.
    :param database: str: The name of the database in MongoDB.
    :param namespace_name: str: The name of the namespace (collection) in MongoDB.
    :param dataset_name: str: The name of the dataset.
    :param cloud_environment: str: The cloud environment where the dataset resides
      (e.g., 'aws', 'gcp').
    :param filename: str: The name of the file to be created in the storage bucket,
    including the file extension (e.g., 'max_partition_value.txt').
    :param storage_bucket_name: str: The name of the cloud storage bucket where the result
    will be uploaded.
    :param remote_log_file: str: The path to a remote log file where operational details and
    metadata will be recorded.
    :param ss: SparkSession: An instance of SparkSession, used for data processing and interactions
    with cloud storage.

    Returns:
        :return: max_estimated_to_injestion int: o valor mÃ¡ximo da coluna de partiÃ§Ã£o
    """

    mensage = (f">>> [INFO] Calculating maximum partition column value {partition_column}")
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)


    from utils import get_schema_to_mongodb_run

    add_security_value = 300000

    custom_schema = get_schema_to_mongodb_run(
        namespace_name,
        dataset_name,
        cloud_environment,
        filename,
        storage_bucket_name,
        remote_log_file,
        ss,
    )
    try:
        df = (
            ss.spark.read.format("mongo")
            .option("uri", sega_url)
            .option("spark.mongodb.input.database", database)
            .option("user", "defaultDbUser")
            .option("spark.mongodb.input.collection", dataset_name)
            .load(schema=custom_schema, inferSchema=False)
            .select(max(col(partition_column)))
        )

        max_result = df.collect()[0][0]

        mensage = (f">>> [INFO] This is the max_result of select: {max_result}")
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        mensage = (f">>> [INFO] with type: {type(max_result)}")
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        if isinstance(max_result, str):
            try:
                max_result = int(max_result)
            except ValueError:

                mensage = f">>> [WARN] max_result ({max_result}) cant be converted to int."
                update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        max_estimated_to_injestion = max_result + add_security_value

        mensage = f">>> [INFO] max result: {max_result}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

        mensage = f">>> [INFO] max estimated to injestion: {max_estimated_to_injestion}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    except Exception as e:
        mensage = f">>> [WARN] Error capturing maximum value {e}"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    return max_estimated_to_injestion


def calc_max_partition_value(
    partition_column,
    dataset_name,
    sega_url,
    sega_user,
    sega_pass,
    ss,
    filename,
    storage_bucket_name,
    remote_log_file,
):
    """
    Calculates the maximum value of the partition column from the source base.

    Args:
    :param partition_column: str: The name of the column used for partitioning the dataset.
    :param dataset_name: str: The name of the dataset.
    :param sega_url: str: The URL of the database server.
    :param sega_user: str: The username for authenticating with the database.
    :param sega_pass: str: The password for authenticating with the database.
    :param ss: SparkSession: An instance of SparkSession, used for data processing.

    Returns:
        :return: max_estimated_to_injestion int: The maximum value of the partition column.
    """

    mensage = (f">>> [INFO] Calculating the maximum value of the division column {partition_column}")
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)


    add_security_value = 300000

    df = (
        ss.spark.read.format("jdbc")
        .option("user", sega_user)
        .option("password", sega_pass)
        .option("url", sega_url)
        .option("dbTable", f"(select max({partition_column}) from {dataset_name}) subs")
        .load()
    )

    max_result = df.collect()[0][0]

    mensage = (f">>> [INFO] this is the max_result of select: {max_result}")
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    mensage = (f">>> [INFO] with type: {type(max_result)}")
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    if isinstance(max_result, str):
        try:
            max_result = int(max_result)
        except ValueError:

            mensage = (f">>> [WARN] max_result ({max_result}) cant be convert to int.")
            update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    max_estimated_to_injestion = max_result + add_security_value

    mensage = (f">>> [INFO] Max value: {max_result}")
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)

    mensage = (f">>> [INFO] estimated max value: {max_estimated_to_injestion}")
    update_logs(mensage, filename, storage_bucket_name, remote_log_file)
    

    return max_estimated_to_injestion

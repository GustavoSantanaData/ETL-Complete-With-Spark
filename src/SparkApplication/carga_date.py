import sys  # noqa
import time  # noqa
from sness.datalake.sness_spark import SnessSpark
from sness.datalake.metadata import Environment, Zone
from datetime import timedelta, datetime
from argparse import ArgumentParser
from pyspark.sql.functions import col, max

from load_utils import run_database_load

from utils import (
    update_logs,
    update_status,
    quantity_level,
    get_sega_driver,
    calculate_allintervals_todefinequery,
    define_start_query,
    define_end_query,
    sets_ssl_false_in_the_connection_string,
)


from cloud_utils import (
    bucket_exists,
    build_logger_writer,
    clear_current_bucket_or_create_a_new_bucket,
)


ss = SnessSpark()

sys.path.append(".")


arguments = ArgumentParser()
arguments.add_argument("--namespace", dest="namespace", type=str, required=True)
arguments.add_argument("--dataset", dest="dataset", type=str, required=True)
arguments.add_argument("--connection", dest="connection", type=str, required=True)
arguments.add_argument("--db_user", dest="db_user", type=str, required=True)
arguments.add_argument("--db_pass", dest="db_pass", type=str, required=True)
arguments.add_argument("--start_process", dest="start", type=str, required=False)
arguments.add_argument("--end_process", dest="end", type=str, required=False)
arguments.add_argument("--id_request", dest="id_request", type=str, required=False)
arguments.add_argument("--partition_column", dest="partition", type=str, required=True)
arguments.add_argument(
    "--cloud_environment", dest="cloud_environment", type=str, required=True
)
arguments.add_argument("--amount", dest="amount", type=str, required=True)

parsed = arguments.parse_args()


namespace_name = parsed.namespace
dataset_name = parsed.dataset

sega_url = parsed.connection
sega_user = parsed.db_user
sega_pass = parsed.db_pass
sega_driver = get_sega_driver(sega_url)

sega_url = sets_ssl_false_in_the_connection_string(sega_driver, sega_url)

id_request = parsed.id_request
partition_column = parsed.partition
quantity = int(parsed.amount)
cloud_environment = parsed.cloud_environment

start_date_str = parsed.start
end_date_str = parsed.end

timedate = datetime.now()
timestamp = timedate.strftime("%Y-%m-%d-%H%M%S")

(
    filename,
    storage_bucket_name,
    remote_log_file,
) = build_logger_writer(namespace_name, dataset_name, timestamp)

mensage = f">> Init process {namespace_name}-{dataset_name} for ID_REQUEST {id_request}"
update_logs(mensage, filename, storage_bucket_name, remote_log_file)

mensage = ">> Checking if the bucket exists in object storage"
update_logs(mensage, filename, storage_bucket_name, remote_log_file)

bucket_already_exists, bucket_name = bucket_exists(
    namespace_name,
    cloud_environment,
    filename,
    storage_bucket_name,
    remote_log_file,
    dataset_name,
)

clear_current_bucket_or_create_a_new_bucket(
    bucket_already_exists,
    bucket_name,
    namespace_name,
    cloud_environment,
    filename,
    storage_bucket_name,
    remote_log_file,
    dataset_name,
)


start_date_query = define_start_query(
    start_date_str,
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
    type_of_load="date",
)


end_date_query = define_end_query(
    end_date_str,
    partition_column,
    dataset_name,
    sega_url,
    sega_user,
    sega_pass,
    ss,
    filename,
    storage_bucket_name,
    remote_log_file,
    type_of_load="date",
)


days_distance_interval_query = quantity_level(
    quantity, start_date_query, end_date_query
)


array_intervals_between_start_end = calculate_allintervals_todefinequery(
    start_date_query,
    end_date_query,
    days_distance_interval_query,
    type_of_process="date",
)


stores_amount_writing_in_lake = 1
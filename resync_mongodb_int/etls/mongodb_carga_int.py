import sys  # noqa
import time  # noqa
from sness.datalake.sness_spark import SnessSpark
from datetime import datetime
from argparse import ArgumentParser

from utils import (
    update_logs,
    update_status,
    get_database_name_inside_string_conection,
    calculate_allintervals_todefinequery,
    get_schema_to_mongodb_run,
    define_start_query_mongodb,
    define_end_query_mongodb,
    quantity_level_int,
)


from cloud_utils import (
    bucket_exists,
    build_logger_writer,
    clear_current_bucket_or_create_a_new_bucket,
)

from load_utils import run_database_load_mongodb

sys.path.append(".")
ss = SnessSpark()


arguments = ArgumentParser()
arguments.add_argument("--namespace", dest="namespace", type=str, required=True)
arguments.add_argument("--dataset", dest="dataset", type=str, required=True)
arguments.add_argument("--connection", dest="connection", type=str, required=True)
arguments.add_argument("--db_user", dest="db_user", type=str, required=False)
arguments.add_argument("--db_pass", dest="db_pass", type=str, required=False)
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
partition_column = parsed.partition
quantity = int(parsed.amount)
id_request = parsed.id_request
cloud_environment = parsed.cloud_environment

start_query_int = parsed.start
end_query_int = parsed.end

print(f"This is ID_REQUEST: {id_request}")


timedate = datetime.now()
timestamp = timedate.strftime("%Y-%m-%d-%H%M%S")


(
    filename,
    storage_bucket_name,
    remote_log_file,
) = build_logger_writer(namespace_name, dataset_name, timestamp)

mensage = f">>> [INFO] Init process {namespace_name}-{dataset_name} for ID_REQUEST {id_request}"
update_logs(mensage, filename, storage_bucket_name, remote_log_file)

mensage = ">>> [INFO] Checking if the bucket exists in object storage"
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


custom_schema = get_schema_to_mongodb_run(
    namespace_name,
    dataset_name,
    cloud_environment,
    filename,
    storage_bucket_name,
    remote_log_file,
    ss,
)


database = get_database_name_inside_string_conection(sega_url, namespace_name)


start_int_query = define_start_query_mongodb(
    start_query_int,
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
    type_of_load="int",
)


end_date_query = define_end_query_mongodb(
    end_query_int,
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
    type_of_load="int",
)


days_distance_interval_query = quantity_level_int(quantity)


array_intervals_between_start_end = calculate_allintervals_todefinequery(
    start_int_query, end_date_query, days_distance_interval_query, type_of_process="int"
)


stores_amount_writing_in_lake = 1




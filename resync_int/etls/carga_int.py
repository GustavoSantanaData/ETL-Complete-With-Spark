import sys  # noqa
import time  # noqa
from sness.datalake.sness_spark import SnessSpark
from sness.datalake.metadata import Environment, Zone
from datetime import datetime
from argparse import ArgumentParser
from pyspark.sql.functions import col, max
from utils import (
    update_logs,
    update_status,
    quantity_level_int,
    calculate_allintervals_todefinequery,
    get_sega_driver,
    define_start_query,
    define_end_query,
    sets_ssl_false_in_the_connection_string,
)


from cloud_utils import (
    bucket_exists,
    build_logger_writer,
    clear_current_bucket_or_create_a_new_bucket,
)

from load_utils import run_database_load

sys.path.append(".")
ss = SnessSpark()


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

start_query_int = parsed.start
end_query_int = parsed.end

print(f"Esse é o ID_REQUEST: {id_request}")

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


start_int_query = define_start_query(
    start_query_int,
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
    type_of_load="int",
)

end_int_query = define_end_query(
    end_query_int,
    partition_column,
    dataset_name,
    sega_url,
    sega_user,
    sega_pass,
    ss,
    filename,
    storage_bucket_name,
    remote_log_file,
    type_of_load="int",
)


distance_interval_query = quantity_level_int(quantity)
array_intervals_between_start_end = calculate_allintervals_todefinequery(
    start_int_query, end_int_query, distance_interval_query, type_of_process="int"
)


stores_amount_writing_in_lake = 1

retry_attempts = 4
updated_intervals = array_intervals_between_start_end.copy()
for interval_index, interval in enumerate(array_intervals_between_start_end):
    for attempt in range(retry_attempts):
        try:
            interval = updated_intervals[interval_index]

            end_num_with_extra_increment = interval[1] + 1

            run_database_load(
                namespace_name,
                dataset_name,
                sega_url,
                interval[0],
                end_num_with_extra_increment,
                partition_column,
                sega_user,
                sega_pass,
                filename,
                storage_bucket_name,
                remote_log_file,
                ss,
                type_of_process="int",
            )

            stores_amount_writing_in_lake += 1

            end_int_query = interval[1]
            break

        except Exception as e:
            print(f">>> [WARN] Error on attempt {attempt + 1}: {str(e)}")
            if attempt == retry_attempts - 1:
                mensage = ">>> [WARN] All attempts failed. Shutting down."
                update_status(id_request, "failed")
                update_logs(mensage, filename, storage_bucket_name, remote_log_file)
                sys.exit(1)

            elif stores_amount_writing_in_lake > 1:
                df_raw = ss.read.parquet(
                    environment=Environment.PRODUCTION,
                    zone=Zone.WORK,
                    namespace=namespace_name,
                    dataset=f"{dataset_name}",
                    cloud_environment=cloud_environment,
                )

                int_retry = df_raw.select(max(col(partition_column))).collect()[0][0]

                start_int_query = int_retry

                int_interval = quantity_level_int(quantity)
                updated_intervals = calculate_allintervals_todefinequery(
                    start_int_query, end_int_query, int_interval, type_of_process="int"
                )

                stores_amount_writing_in_lake = 1

                print(f">>> [INFO] Trying again from {int_retry}...")

                time.sleep(60)
                break
            else:
                mensage = ">>> [INFO] Trying again..."
                update_logs(mensage, filename, storage_bucket_name, remote_log_file)
                time.sleep(60)

    array_intervals_between_start_end = updated_intervals

    if stores_amount_writing_in_lake > len(array_intervals_between_start_end):
        update_status(id_request, "success")
        mensage = ">>> [INFO] Process finished successfully"
        update_logs(mensage, filename, storage_bucket_name, remote_log_file)
        break
ss.spark.stop()

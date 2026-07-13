import os
import json
import sys
import uuid
import logging
import argparse
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import pyarrow as pa
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from deltalake import DeltaTable, write_deltalake


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(threadName)s %(message)s"
)
logger = logging.getLogger(__name__)


def get_arg(name, default=None):
    arg_name = f"--{name}"

    if arg_name not in sys.argv:
        return default

    index = sys.argv.index(arg_name)

    if index + 1 >= len(sys.argv):
        raise ValueError(f"Missing value for argument {arg_name}")

    return sys.argv[index + 1]


def load_config(config_arg):
    config_arg = config_arg.strip().strip('"').strip("'")
    config_path = os.path.abspath(config_arg)

    if os.path.isfile(config_path):
        with open(config_path, "r", encoding="utf-8") as file:
            return json.load(file)

    try:
        return json.loads(config_arg)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"--config must be a valid file path or JSON string: {exc}"
        ) from exc

config_arg = get_arg("config")
window_start = get_arg("window_start")
window_end = get_arg("window_end")

if not config_arg:
    raise ValueError("--config is required")

if not window_start:
    raise ValueError("--window_start is required")

if not window_end:
    raise ValueError("--window_end is required")


CONFIG = load_config(config_arg)


AWS_ENDPOINT = CONFIG["aws_endpoint"]
CLIENT_ID = CONFIG["client_id"]
POSTGRES_CONFIG = CONFIG["db_details"]
DELTA_PATH = CONFIG["delta_path"].rstrip("/")
COLLECTIONS_CONFIG = CONFIG["collections_config"]
MAX_WORKERS = int(CONFIG.get("max_threads", 3))

AWS_ACCESS_KEY = (
    CONFIG.get("aws_access_key_id")
    or os.getenv("AWS_ACCESS_KEY_ID")
    or "test"
)

AWS_SECRET_KEY = (
    CONFIG.get("aws_secret_access_key")
    or os.getenv("AWS_SECRET_ACCESS_KEY")
    or "test"
)

AWS_REGION = (
    CONFIG.get("aws_region")
    or os.getenv("AWS_REGION")
    or "us-east-1"
)

STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_KEY,
    "AWS_REGION": AWS_REGION,
    "AWS_ENDPOINT_URL": AWS_ENDPOINT,
    "AWS_ALLOW_HTTP": "true",
    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",
}



def normalize_s3_path(path: str) -> str:
    return path.replace("s3a://", "s3://").replace("s3n://", "s3://")


def parse_s3_path(path: str):
    path = normalize_s3_path(path)
    if not path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {path}")

    path = path[5:]
    parts = path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def read_json_from_s3(s3_client, s3_path: str):
    bucket, key = parse_s3_path(s3_path)
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def deduplicate_records(records, merge_key):
    deduped = {}

    for record in records:
        key = record.get(merge_key)
        if key is None:
            continue

        key = str(key)
        current_ts = int(record.get("__source_ts_ms") or 0)

        existing = deduped.get(key)
        if existing is None:
            deduped[key] = record
            continue

        existing_ts = int(existing.get("__source_ts_ms") or 0)
        if current_ts >= existing_ts:
            deduped[key] = record

    return list(deduped.values())


def read_existing_delta_table(delta_path, storage_options):
    if not DeltaTable.is_deltatable(delta_path, storage_options=storage_options):
        return []

    dt = DeltaTable(delta_path, storage_options=storage_options)
    table = dt.to_pyarrow_table()
    records = table.to_pylist()

    logger.info(
        "Existing Delta table found: path=%s version=%s records=%s",
        delta_path, dt.version(), len(records)
    )
    return records


def get_delta_partition_path(delta_path, client_id, microservice, collection_name, window_start):
    dt = datetime.strptime(window_start, "%Y-%m-%d %H:%M:%S")
    partition = dt.strftime("%Y%m%d%H0000")

    return (
        f"{delta_path.rstrip('/')}/"
        f"{client_id}/"
        f"{microservice}/"
        f"{collection_name}/"
        f"date={partition}"
    )


class PostgresPool:
    def __init__(self, db_config, minconn=1, maxconn=10):
        self.pool = ThreadedConnectionPool(minconn=minconn, maxconn=maxconn, **db_config)

    def get_conn(self):
        return self.pool.getconn()

    def put_conn(self, conn):
        self.pool.putconn(conn)

    def closeall(self):
        self.pool.closeall()


def execute_query(pg_pool, query, params=None, fetch=False):
    conn = pg_pool.get_conn()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                if fetch:
                    return cur.fetchall()
    finally:
        pg_pool.put_conn(conn)


def insert_step_metric(
    pg_pool,
    job_id,
    client_id,
    collection_name,
    step_name,
    window_start,
    window_end,
    input_count=0,
    unique_count=0,
    rejected_count=0,
    output_count=0,
    last_partition_count=0,
    current_delta_count=0,
    started_at=None,
    ended_at=None,
):
    execute_query(
        pg_pool,
        """
        INSERT INTO delta_step_metrics (
            job_id, client_id, collection_name, step_name, window_start, window_end,
            input_count, unique_count, rejected_count, output_count,
            last_partition_count, current_delta_count, started_at, ended_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            job_id, client_id, collection_name, step_name, window_start, window_end,
            input_count, unique_count, rejected_count, output_count,
            last_partition_count, current_delta_count, started_at, ended_at
        )
    )


def get_rawzone_paths(pg_pool, client_id, collection_name, window_start, window_end):
    rows = execute_query(
        pg_pool,
        """
        SELECT DISTINCT rawzone_path
        FROM rawzone_extraction_metadata
        WHERE client_id = %s
          AND collection = %s
          AND event_ts >= %s
          AND event_ts < %s
        ORDER BY rawzone_path
        """,
        (client_id, collection_name, window_start, window_end),
        fetch=True,
    )
    return [normalize_s3_path(row[0]) for row in rows]


def validate_records(records, merge_key):
    valid_records = []
    rejected_records = []

    required_columns = {
        "event_type",
        merge_key,
        "client_id",
        "payload",
        "updated_at",
        "__op",
        "__source_ts_ms",
    }

    for record in records:
        if not required_columns.issubset(record.keys()):
            rejected_records.append(record)
            continue

        if (
            record.get(merge_key) is None
            or record.get("client_id") is None
            or record.get("payload") is None
        ):
            rejected_records.append(record)
            continue

        valid_records.append(record)

    return valid_records, rejected_records


def get_previous_delta_partition_path(
    delta_path,
    client_id,
    microservice,
    collection_name,
    window_start,
):
    current_window = datetime.strptime(
        window_start,
        "%Y-%m-%d %H:%M:%S",
    )

    previous_window = current_window - timedelta(hours=1)
    previous_partition = previous_window.strftime("%Y%m%d%H0000")

    return (
        f"{delta_path.rstrip('/')}/"
        f"{client_id}/"
        f"{microservice}/"
        f"{collection_name}/"
        f"date={previous_partition}"
    )

def process_collection(
    collection_name,
    collection_conf,
    client_id,
    delta_path,
    window_start,
    window_end,
    s3_client,
    pg_pool,
    storage_options,
):
    job_id = f"{collection_name}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
    microservice = collection_conf["microservice"]
    merge_key = collection_conf.get("merge_key", "key")

    collection_delta_path = get_delta_partition_path(
        delta_path, client_id, microservice, collection_name, window_start
    )

    logger.info("Started collection=%s microservice=%s job_id=%s", collection_name, microservice, job_id)

    try:
        step_start = datetime.now(timezone.utc)
        rawzone_paths = get_rawzone_paths(pg_pool, client_id, collection_name, window_start, window_end)

        if not rawzone_paths:
            step_end = datetime.now(timezone.utc)
            insert_step_metric(
                pg_pool, job_id, client_id, collection_name, "read_rawzone",
                window_start, window_end, started_at=step_start, ended_at=step_end
            )
            logger.info("No rawzone files found for collection=%s", collection_name)
            return

        raw_records = [read_json_from_s3(s3_client, path) for path in rawzone_paths]
        input_count = len(raw_records)
        unique_count = len(deduplicate_records(raw_records, merge_key))
        step_end = datetime.now(timezone.utc)

        insert_step_metric(
            pg_pool, job_id, client_id, collection_name, "read_rawzone",
            window_start, window_end,
            input_count=input_count,
            unique_count=unique_count,
            output_count=input_count,
            started_at=step_start,
            ended_at=step_end,
        )

        step_start = datetime.now(timezone.utc)
        valid_records, rejected_records = validate_records(raw_records, merge_key)
        valid_unique_count = len(deduplicate_records(valid_records, merge_key))
        step_end = datetime.now(timezone.utc)

        insert_step_metric(
            pg_pool, job_id, client_id, collection_name, "schema_validation",
            window_start, window_end,
            input_count=input_count,
            unique_count=valid_unique_count,
            rejected_count=len(rejected_records),
            output_count=len(valid_records),
            started_at=step_start,
            ended_at=step_end,
        )

        if not valid_records:
            logger.info("No valid records after schema validation for collection=%s", collection_name)
            return

        step_start = datetime.now(timezone.utc)
        previous_delta_path = get_previous_delta_partition_path(
            delta_path=delta_path,
            client_id=client_id,
            microservice=microservice,
            collection_name=collection_name,
            window_start=window_start,
        )

        logger.info(
            "Checking previous Delta partition: collection=%s path=%s",
            collection_name,
            previous_delta_path,
        )

        old_records = read_existing_delta_table(
            previous_delta_path,
            storage_options,
        )
        last_partition_count = len(old_records)

        merged_records = deduplicate_records(old_records + valid_records, merge_key)
        merge_output_count = len(merged_records)
        step_end = datetime.now(timezone.utc)

        insert_step_metric(
            pg_pool, job_id, client_id, collection_name, "merge_delta",
            window_start, window_end,
            input_count=len(old_records) + len(valid_records),
            unique_count=merge_output_count,
            rejected_count=max(0, len(valid_records) - valid_unique_count),
            output_count=merge_output_count,
            last_partition_count=last_partition_count,
            current_delta_count=merge_output_count,
            started_at=step_start,
            ended_at=step_end,
        )

        if not merged_records:
            logger.info("No records available to write for collection=%s", collection_name)
            return

        step_start = datetime.now(timezone.utc)
        loaded_at = datetime.now(timezone.utc)

        final_records = []
        for record in merged_records:
            rec = dict(record)
            rec["delta_loaded_at"] = loaded_at.isoformat()
            final_records.append(rec)

        arrow_table = pa.Table.from_pylist(final_records)

        write_deltalake(
            collection_delta_path,
            arrow_table,
            mode="overwrite",
            storage_options=storage_options,
        )

        written_table = DeltaTable(collection_delta_path, storage_options=storage_options)
        current_delta_count = len(final_records)
        step_end = datetime.now(timezone.utc)

        insert_step_metric(
            pg_pool, job_id, client_id, collection_name, "write_delta",
            window_start, window_end,
            input_count=merge_output_count,
            unique_count=current_delta_count,
            output_count=current_delta_count,
            last_partition_count=last_partition_count,
            current_delta_count=current_delta_count,
            started_at=step_start,
            ended_at=step_end,
        )

        logger.info(
            "Completed collection=%s job_id=%s delta_path=%s version=%s records=%s",
            collection_name, job_id, collection_delta_path, written_table.version(), current_delta_count
        )

    except Exception as e:
        error_time = datetime.now(timezone.utc)
        insert_step_metric(
            pg_pool, job_id, client_id, collection_name, "job_failed",
            window_start, window_end,
            started_at=error_time,
            ended_at=error_time,
        )
        logger.exception("Failed collection=%s error=%s", collection_name, str(e))
        raise

def main():

    s3_client = boto3.client(
        "s3",
        endpoint_url=AWS_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION,
    )

    pg_pool = PostgresPool(
        db_config=POSTGRES_CONFIG,
        minconn=1,
        maxconn=MAX_WORKERS + 2,
    )

    try:
        print("Starting Delta Loader")
        print(f"Client: {CLIENT_ID}")
        print(f"Window: {window_start} to {window_end}")
        print(f"Collections: {list(COLLECTIONS_CONFIG.keys())}")
        print(f"Max threads: {MAX_WORKERS}")

        failed_collections = []

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {
                executor.submit(
                    process_collection,
                    collection_name,
                    collection_conf,
                    CLIENT_ID,
                    DELTA_PATH,
                    window_start,
                    window_end,
                    s3_client,
                    pg_pool,
                    STORAGE_OPTIONS,
                ): collection_name
                for collection_name, collection_conf
                in COLLECTIONS_CONFIG.items()
            }

            for future in as_completed(future_map):
                collection_name = future_map[future]

                try:
                    future.result()
                except Exception as exc:
                    failed_collections.append(collection_name)
                    print(
                        f"Collection failed: {collection_name}, "
                        f"error={exc}"
                    )

        if failed_collections:
            raise RuntimeError(
                f"Delta loader failed for collections: "
                f"{failed_collections}"
            )

        print("Delta Loader completed successfully")
    
    finally:
        pg_pool.closeall()
        print("Postgres connection pool closed")

if __name__ == "__main__":
    main()
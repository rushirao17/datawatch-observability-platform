import os
import json
import sys
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pyarrow as pa
from deltalake import write_deltalake
import boto3
import psycopg2


AWS_ACCESS_KEY = "test"
AWS_SECRET_KEY = "test"


def get_arg(name, default=None):
    arg_name = f"--{name}"
    if arg_name in sys.argv:
        return sys.argv[sys.argv.index(arg_name) + 1]
    return default


config_arg = get_arg("config")
window_start = get_arg("window_start")
window_end = get_arg("window_end")

if not config_arg:
    raise Exception("--config is required")

if not window_start or not window_end:
    raise Exception("--window_start and --window_end are required")

config_arg = config_arg.strip().strip('"').strip("'")
config_path = os.path.abspath(config_arg)

if os.path.isfile(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        CONFIG = json.load(f)
else:
    CONFIG = json.loads(config_arg)


AWS_ENDPOINT = CONFIG["aws_endpoint"]
CLIENT_ID = CONFIG["client_id"]
POSTGRES_CONFIG = CONFIG["db_details"]
DELTA_PATH = CONFIG["delta_path"].rstrip("/")
COLLECTIONS_CONFIG = CONFIG["collections_config"]
MAX_WORKERS = int(CONFIG.get("max_threads", 3))


s3 = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
)



def s3_path_to_prefix(path):
    bucket, key = parse_s3_path(path)
    if key and not key.endswith("/"):
        key += "/"
    return bucket, key


def read_json_from_s3(s3_path):
    bucket, key = parse_s3_path(s3_path)
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return json.loads(body)


def write_json_to_s3(s3_path, data):
    bucket, key = parse_s3_path(s3_path)
    body = json.dumps(data, default=str, indent=2)
    s3.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"))


def list_s3_json_files(s3_prefix_path):
    bucket, prefix = s3_path_to_prefix(s3_prefix_path)

    files = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                files.append(f"s3://{bucket}/{key}")

    return files


def get_db_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


def insert_step_metric(
    job_id,
    client_id,
    collection_name,
    step_name,
    input_count=0,
    unique_count=0,
    rejected_count=0,
    output_count=0,
    last_partition_count=0,
    current_delta_count=0,
    started_at=None,
    ended_at=None,
):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO delta_step_metrics (job_id,client_id, collection_name, step_name, window_start, 
                                        window_end, input_count, unique_count, rejected_count, 
                                        output_count, last_partition_count, current_delta_count, 
                                        started_at, ended_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                                        (job_id,CLIENT_ID, collection_name, step_name, window_start, 
                                        window_end, input_count, unique_count, rejected_count, 
                                        output_count, last_partition_count, current_delta_count, 
                                        started_at, ended_at))

    conn.commit()
    cur.close()
    conn.close()


def get_rawzone_paths(collection_name):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT DISTINCT rawzone_path
        FROM rawzone_extraction_metadata
        WHERE client_id = %s
          AND collection = %s
          AND event_ts >= %s
          AND event_ts < %s
        ORDER BY rawzone_path
        """,
        (CLIENT_ID, collection_name, window_start, window_end),
    )

    paths = [normalize_s3_path(row[0]) for row in cur.fetchall()]

    cur.close()
    conn.close()

    return paths

def deduplicate_records(records, merge_key):
    deduped = {}

    for record in records:
        key = record.get(merge_key)
        if key is None:
            continue

        existing = deduped.get(str(key))

        if existing is None:
            deduped[str(key)] = record
            continue

        existing_ts = existing.get("__source_ts_ms") or 0
        current_ts = record.get("__source_ts_ms") or 0

        if int(current_ts) >= int(existing_ts):
            deduped[str(key)] = record

    return list(deduped.values())

def normalize_s3_path(path):
    return (
        path.replace("s3a://", "s3://")
            .replace("s3n://", "s3://")
    )


def parse_s3_path(path):
    path = normalize_s3_path(path)

    if not path.startswith("s3://"):
        raise Exception(f"Invalid S3 path: {path}")

    path = path.replace("s3://", "", 1)
    bucket = path.split("/")[0]
    key = "/".join(path.split("/")[1:])

    return bucket, key


def delete_old_step_metrics(collection_name):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        DELETE FROM delta_step_metrics
        WHERE client_id = %s
          AND collection_name = %s
          AND window_start = %s
          AND window_end = %s
        """,
        (CLIENT_ID, collection_name, window_start, window_end)
    )

    conn.commit()
    cur.close()
    conn.close()

    print(f"Deleted old metrics for {collection_name}, {window_start} to {window_end}")


def get_delta_partition_path( delta_path, client_id, microservice, collection_name, window_start,):
    dt = datetime.strptime(window_start, "%Y-%m-%d %H:%M:%S")

    partition = dt.strftime("%Y%m%d%H0000")

    return (
        f"{delta_path}/"
        f"{client_id}/"
        f"{microservice}/"
        f"{collection_name}/"
        f"date={partition}/"
    )

def delete_s3_prefix(s3_prefix_path):
    bucket, prefix = s3_path_to_prefix(s3_prefix_path)

    paginator = s3.get_paginator("list_objects_v2")
    objects = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append({"Key": obj["Key"]})

    if objects:
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": objects}
        )
        print(f"Deleted old delta partition: {s3_prefix_path}")

def process_collection(collection_name, collection_conf):
    job_id = f"{collection_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"

    microservice = collection_conf["microservice"]
    merge_key = collection_conf.get("merge_key", "key")

    collection_delta_prefix = get_delta_partition_path(
        DELTA_PATH,
        CLIENT_ID,
        microservice,
        collection_name,
        window_start,
    )

    delete_old_step_metrics(collection_name)
    delete_s3_prefix(collection_delta_prefix)

    print(f"Started collection={collection_name}, microservice={microservice}, job_id={job_id}")

    try:
        # --------------------------------------------------
        # STEP 1: READ RAWZONE
        # --------------------------------------------------

        step_start = datetime.now()

        rawzone_paths = get_rawzone_paths(collection_name)

        if not rawzone_paths:
            step_end = datetime.now()

            insert_step_metric(
                job_id=job_id,
                client_id=CLIENT_ID,
                collection_name=collection_name,
                step_name="read_rawzone",
                input_count=0,
                unique_count=0,
                rejected_count=0,
                output_count=0,
                started_at=step_start,
                ended_at=step_end,
            )

            print(f"No rawzone files found for collection={collection_name}")
            return

        raw_records = []

        for path in rawzone_paths:
            record = read_json_from_s3(path)
            raw_records.append(record)

        input_count = len(raw_records)
        unique_count = len(deduplicate_records(raw_records, merge_key))

        step_end = datetime.now()

        insert_step_metric(
            job_id=job_id,
            client_id=CLIENT_ID,
            collection_name=collection_name,
            step_name="read_rawzone",
            input_count=input_count,
            unique_count=unique_count,
            rejected_count=0,
            output_count=input_count,
            started_at=step_start,
            ended_at=step_end,
        )

        # --------------------------------------------------
        # STEP 2: SCHEMA VALIDATION
        # --------------------------------------------------

        step_start = datetime.now()

        valid_records = []
        rejected_records = []

        required_columns = [
            "event_type",
            "key",
            "client_id",
            "payload",
            "updated_at",
            "__op",
            "__source_ts_ms",
        ]

        for record in raw_records:
            missing_columns = [c for c in required_columns if c not in record]

            if missing_columns:
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

        output_count = len(valid_records)
        rejected_count = len(rejected_records)
        valid_unique_count = len(deduplicate_records(valid_records, merge_key))

        step_end = datetime.now()

        insert_step_metric(
            job_id=job_id,
            client_id=CLIENT_ID,
            collection_name=collection_name,
            step_name="schema_validation",
            input_count=input_count,
            unique_count=valid_unique_count,
            rejected_count=rejected_count,
            output_count=output_count,
            started_at=step_start,
            ended_at=step_end,
        )

        if output_count == 0:
            print(f"No valid records after schema validation for collection={collection_name}")
            return

        # --------------------------------------------------
        # STEP 3: MERGE WITH LAST DELTA PARTITION
        # --------------------------------------------------

        step_start = datetime.now()

        old_delta_files = list_s3_json_files(collection_delta_prefix)

        old_records = []

        for path in old_delta_files:
            old_data = read_json_from_s3(path)

            if isinstance(old_data, list):
                old_records.extend(old_data)
            else:
                old_records.append(old_data)

        last_partition_count = len(old_records)

        merged_records = deduplicate_records(old_records + valid_records, merge_key)

        merge_input_count = output_count
        merge_output_count = len(merged_records)
        merge_unique_count = merge_output_count
        merge_rejected_count = merge_input_count - valid_unique_count

        step_end = datetime.now()

        insert_step_metric(
            job_id=job_id,
            client_id=CLIENT_ID,
            collection_name=collection_name,
            step_name="merge_last_partition",
            input_count=merge_input_count,
            unique_count=merge_unique_count,
            rejected_count=merge_rejected_count,
            output_count=merge_output_count,
            last_partition_count=last_partition_count,
            current_delta_count=merge_output_count,
            started_at=step_start,
            ended_at=step_end,
        )

        # --------------------------------------------------
        # STEP 4: WRITE DELTA
        # --------------------------------------------------

        step_start = datetime.now()

        loaded_at = datetime.now().isoformat()

        final_records = []

        for record in merged_records:
            record["delta_loaded_at"] = loaded_at
            final_records.append(record)

        table = pa.Table.from_pylist(final_records)

        write_deltalake(
            collection_delta_prefix,
            table,
            mode="overwrite"
        )

        current_delta_count = len(final_records)

        step_end = datetime.now()

        insert_step_metric(
            job_id=job_id,
            client_id=CLIENT_ID,
            collection_name=collection_name,
            step_name="write_delta",
            input_count=merge_output_count,
            unique_count=current_delta_count,
            rejected_count=0,
            output_count=current_delta_count,
            last_partition_count=last_partition_count,
            current_delta_count=current_delta_count,
            started_at=step_start,
            ended_at=step_end,
        )

        print(f"Completed collection={collection_name}, job_id={job_id}")
        print(f"Written delta output: {output_file}")

    except Exception as e:
        error_time = datetime.now()

        insert_step_metric(
            job_id=job_id,
            client_id=CLIENT_ID,
            collection_name=collection_name,
            step_name="job_failed",
            started_at=error_time,
            ended_at=error_time,
        )

        print(f"Failed collection={collection_name}, error={str(e)}")
        raise e


try:
    print("Starting Delta Loader")
    print(f"Window: {window_start} to {window_end}")
    print(f"Collections: {list(COLLECTIONS_CONFIG.keys())}")
    print(f"Max threads: {MAX_WORKERS}")

    failed_collections = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(process_collection, collection_name, collection_conf): collection_name
            for collection_name, collection_conf in COLLECTIONS_CONFIG.items()
        }

        for future in as_completed(future_map):
            collection_name = future_map[future]

            try:
                future.result()
            except Exception as e:
                failed_collections.append(collection_name)
                print(f"Collection failed: {collection_name}, error={str(e)}")

    if failed_collections:
        raise Exception(f"Delta loader failed for collections: {failed_collections}")

    print("Delta Loader completed successfully")

finally:
    print("Loader finished")
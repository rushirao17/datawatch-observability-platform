import json

from datetime import datetime



import boto3

import pyarrow as pa

from deltalake import write_deltalake





AWS_ENDPOINT = "http://localhost:4566"



STORAGE_OPTIONS = {

    "AWS_ACCESS_KEY_ID": "test",

    "AWS_SECRET_ACCESS_KEY": "test",

    "AWS_REGION": "us-east-1",

    "AWS_ENDPOINT_URL": AWS_ENDPOINT,

    "AWS_ALLOW_HTTP": "true",

    "AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "false",

}



s3 = boto3.client(

    "s3",

    endpoint_url=AWS_ENDPOINT,

    aws_access_key_id="test",

    aws_secret_access_key="test",

    region_name="us-east-1",

)





def read_json_prefix(bucket: str, prefix: str) -> list[dict]:

    records = []

    paginator = s3.get_paginator("list_objects_v2")



    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):

        for item in page.get("Contents", []):

            key = item["Key"]



            if not key.endswith(".json"):

                continue



            response = s3.get_object(Bucket=bucket, Key=key)

            data = json.loads(response["Body"].read().decode("utf-8"))



            if isinstance(data, list):

                records.extend(data)

            else:

                records.append(data)



    return records





def deduplicate_records(records: list[dict], merge_key: str) -> list[dict]:

    latest = {}



    for record in records:

        key = record.get(merge_key)



        if key is None:

            continue



        current_ts = int(record.get("__source_ts_ms") or 0)

        previous = latest.get(str(key))



        if previous is None:

            latest[str(key)] = record

            continue



        previous_ts = int(previous.get("__source_ts_ms") or 0)



        if current_ts >= previous_ts:

            latest[str(key)] = record



    return list(latest.values())





source_bucket = "delta"

source_prefix = "client1/microservice1/orders/"



target_uri = "s3://delta/deltaTables/client1/microservice1/orders"



records = read_json_prefix(source_bucket, source_prefix)



if not records:

    raise RuntimeError("No JSON records found for migration")



records = deduplicate_records(records, merge_key="key")



migration_time = datetime.now().isoformat()



for record in records:

    record["delta_loaded_at"] = migration_time

    record["migration_source"] = "legacy_json"



arrow_table = pa.Table.from_pylist(records)



write_deltalake(

    target_uri,

    arrow_table,

    mode="overwrite",

    storage_options=STORAGE_OPTIONS,

)



print(f"Migrated {len(records)} records")

print(f"Delta table created at: {target_uri}")
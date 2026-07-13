import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import boto3
import psycopg2
from botocore.exceptions import ClientError
from kafka import KafkaConsumer


KAFKA_TOPIC = "pg_rb.public.orders"

POSTGRES_CONFIG = {
    "host": "localhost",
    "database": "ecommerceapp",
    "user": "postgres",
    "password": "root",
    "port": 5432,
}

AWS_ENDPOINT = "http://localhost:4566"
AWS_ACCESS_KEY = "test"
AWS_SECRET_KEY = "test"
AWS_REGION = "ap-south-1"
RAWZONE_BUCKET = "rawzone"

IST = ZoneInfo("Asia/Kolkata")


s3 = boto3.client(
    "s3",
    endpoint_url=AWS_ENDPOINT,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=AWS_REGION,
)


consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers="localhost:9092",
    group_id="rawzone-writer",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)


def ensure_bucket(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
    except ClientError:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket created: {bucket_name}")


def get_now_ist():
    return datetime.now(IST).replace(tzinfo=None)


def get_event_ts(event):
    """
    Returns event timestamp in IST as naive timestamp.
    This event_ts will be used for:
    - rawzone partition date/hour
    - metadata event_ts
    - delta loader windows
    - Superset dashboard
    """
    ts_ms = event.get("__source_ts_ms")

    if ts_ms:
        return (
            datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc)
            .astimezone(IST)
            .replace(tzinfo=None)
        )

    return get_now_ist()


def insert_metadata(
    key_id,
    client_id,
    collection,
    event_ts,
    ingestion_ts,
    size,
    rawzone_path,
):
    conn = psycopg2.connect(**POSTGRES_CONFIG)
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO rawzone_extraction_metadata
        (
            key_id,
            client_id,
            collection,
            event_ts,
            ingestion_ts,
            size,
            rawzone_path
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,
        (
            key_id,
            client_id,
            collection,
            event_ts,
            ingestion_ts,
            size,
            rawzone_path,
        ),
    )

    conn.commit()
    cur.close()
    conn.close()


def build_rawzone_event(event, event_ts, ingestion_ts):
    return {
        "event_type": event.get("__table"),
        "key": event.get("order_id"),
        "client_id": event.get("client_id"),
        "event_ts": str(event_ts),
        "ingestion_ts": str(ingestion_ts),
        "payload": {
            "order_id": event.get("order_id"),
            "customer_id": event.get("customer_id"),
            "amount": event.get("amount"),
            "status": event.get("status"),
            "created_at": event.get("created_at"),
        },
        "updated_at": event.get("updated_at"),
        "__op": event.get("__op"),
        "__source_ts_ms": event.get("__source_ts_ms"),
    }


def write_to_rawzone(event):
    ensure_bucket(RAWZONE_BUCKET)

    collection = event.get("__table", "orders")
    client_id = event.get("client_id", "unknown_client")
    key_id = event.get("order_id")

    if key_id is None:
        print("Skipping event because order_id is missing:", event)
        return

    event_ts = get_event_ts(event)
    ingestion_ts = get_now_ist()

    date_str = event_ts.strftime("%Y%m%d")
    hour_str = event_ts.strftime("%H")

    object_key = (
        f"{client_id}/"
        f"{collection}/"
        f"date={date_str}/"
        f"hour={hour_str}/"
        f"{key_id}.json"
    )

    rawzone_event = build_rawzone_event(
        event=event,
        event_ts=event_ts,
        ingestion_ts=ingestion_ts,
    )

    body = json.dumps(rawzone_event, default=str)

    s3.put_object(
        Bucket=RAWZONE_BUCKET,
        Key=object_key,
        Body=body.encode("utf-8"),
    )

    rawzone_path = f"s3://{RAWZONE_BUCKET}/{object_key}"
    size = len(body.encode("utf-8"))

    insert_metadata(
        key_id=key_id,
        client_id=client_id,
        collection=collection,
        event_ts=event_ts,
        ingestion_ts=ingestion_ts,
        size=size,
        rawzone_path=rawzone_path,
    )

    print(
        f"Written: {rawzone_path} | "
        f"event_ts={event_ts} | ingestion_ts={ingestion_ts}"
    )


print("Rawzone consumer started...")
print(f"Topic: {KAFKA_TOPIC}")

for message in consumer:
    event = message.value
    write_to_rawzone(event)
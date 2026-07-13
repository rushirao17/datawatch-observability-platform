import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

TOPIC = "rawzone-events"

collections = ["orders", "payments", "customers"]

while True:
    collection = random.choice(collections)

    if collection == "orders":
        key_id = f"ORD{random.randint(1, 20)}"
        data = {
            "order_id": key_id,
            "customer_id": f"CUST{random.randint(1, 10)}",
            "amount": random.randint(500, 5000),
            "status": random.choice(["PLACED", "SHIPPED", "DELIVERED"])
        }

    elif collection == "payments":
        key_id = f"PAY{random.randint(1, 20)}"
        data = {
            "payment_id": key_id,
            "order_id": f"ORD{random.randint(1, 20)}",
            "amount": random.randint(500, 5000),
            "payment_status": random.choice(["SUCCESS", "FAILED"])
        }

    else:
        key_id = f"CUST{random.randint(1, 10)}"
        data = {
            "customer_id": key_id,
            "name": random.choice(["Amit", "Ravi", "Sneha", "Priya"]),
            "city": random.choice(["Mumbai", "Pune", "Nashik"])
        }

    event = {
        "metadata": {
            "key_id": key_id,
            "client_id": "client1",
            "collection": collection,
            "event_time": datetime.now().isoformat(timespec="seconds")
        },
        "data": data
    }

    producer.send(TOPIC, event)
    print("Produced:", event)

    time.sleep(2)
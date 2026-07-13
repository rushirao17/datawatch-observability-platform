import random
import string
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import psycopg2
from flask import Flask, request, redirect, render_template_string


app = Flask(__name__)

POSTGRES_CONFIG = {
    "host": "localhost",
    "database": "ecommerceapp",
    "user": "postgres",
    "password": "root",
    "port": 5432,
}


HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>DataWatch Event Generator</title>
    <style>
        body { font-family: Arial; margin: 40px; background: #f7f7f7; }
        .card { background: white; padding: 25px; border-radius: 10px; width: 700px; }
        button { padding: 12px 18px; margin: 8px; border: none; border-radius: 6px; cursor: pointer; }
        .green { background: #22c55e; color: white; }
        .yellow { background: #eab308; color: black; }
        .red { background: #ef4444; color: white; }
        .blue { background: #3b82f6; color: white; }
        .purple { background: #8b5cf6; color: white; }
        input, select { padding: 8px; margin: 8px; width: 180px; }
    </style>
</head>
<body>
<div class="card">
    <h2>DataWatch Event Generator</h2>

    <form method="POST" action="/generate">
        <label>Client</label>
        <select name="client_id">
            <option value="client1">client1</option>
            <option value="client2">client2</option>
            <option value="client3">client3</option>
        </select>

        <label>Records</label>
        <input type="number" name="records" value="10">

        <br><br>

        <button class="green" name="scenario" value="normal">10 / N Normal Orders</button>
        <button class="yellow" name="scenario" value="duplicates">Duplicate Orders</button>
        <button class="red" name="scenario" value="null_customer">Null Customer IDs</button>
        <button class="blue" name="scenario" value="late_2h">Late Data - 2 Hours Old</button>
        <button class="blue" name="scenario" value="late_1d">Late Data - 1 Day Old</button>
        <button class="purple" name="scenario" value="mixed">Mixed Dataset</button>
    </form>

    <hr>

    <h3>Custom Window Generator</h3>

    <form method="POST" action="/generate_window">
        <label>Client</label>
        <select name="client_id">
            <option value="client1">client1</option>
            <option value="client2">client2</option>
            <option value="client3">client3</option>
        </select>

        <label>Records</label>
        <input type="number" name="records" value="20">

        <br>

        <label>Date</label>
        <input type="date" name="event_date">

        <label>Hour</label>
        <input type="number" name="event_hour" value="14" min="0" max="23">

        <br>

        <button class="green">Generate For Selected Window</button>
    </form>

    {% if message %}
        <h3>{{ message }}</h3>
    {% endif %}
</div>
</body>
</html>
"""


def update_random_order(client_id, updated_at):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT order_id
        FROM orders
        WHERE client_id = %s
        ORDER BY random()
        LIMIT 1
        """,
        (client_id,),
    )

    row = cur.fetchone()

    if not row:
        cur.close()
        conn.close()
        return False

    order_id = row[0]

    cur.execute(
        """
        UPDATE orders
        SET amount = %s,
            status = %s,
            updated_at = %s
        WHERE order_id = %s
        """,
        (
            round(random.uniform(500, 10000), 2),
            random.choice(["PLACED", "CONFIRMED", "SHIPPED", "DELIVERED"]),
            updated_at,
            order_id,
        ),
    )

    conn.commit()
    cur.close()
    conn.close()
    
    return True

def get_conn():
    return psycopg2.connect(**POSTGRES_CONFIG)


def random_customer_id():
    return "CUST" + "".join(random.choices(string.digits, k=4))


def insert_order(client_id, customer_id, amount, status, created_at, updated_at):
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO orders
        (
            client_id,
            customer_id,
            amount,
            status,
            created_at,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            client_id,
            customer_id,
            amount,
            status,
            created_at,
            updated_at,
        ),
    )

    conn.commit()
    cur.close()
    conn.close()


def generate_orders(client_id, records, scenario, base_time=None):
    IST = ZoneInfo("Asia/Kolkata")

    if base_time is None:
        base_time = datetime.now(IST).replace(tzinfo=None)

    duplicate_customer = random_customer_id()

    for i in range(records):
        amount = round(random.uniform(500, 10000), 2)
        status = random.choice(["PLACED", "CONFIRMED", "SHIPPED", "DELIVERED"])

        event_time = base_time + timedelta(seconds=i)

        if scenario == "normal":
            customer_id = random_customer_id()

        elif scenario == "duplicates":
            if i % 3 == 0:
                updated = update_random_order(client_id, event_time)
                if updated:
                    continue

            customer_id = random_customer_id()

        elif scenario == "null_customer":
            customer_id = None if i % 4 == 0 else random_customer_id()

        elif scenario == "late_2h":
            customer_id = random_customer_id()
            event_time = datetime.now(IST).replace(tzinfo=None) - timedelta(hours=2, seconds=i)

        elif scenario == "late_1d":
            customer_id = random_customer_id()
            event_time = datetime.now(IST).replace(tzinfo=None) - timedelta(days=1, seconds=i)

        elif scenario == "mixed":
            customer_id = None if i % 5 == 0 else random_customer_id()
            if i % 7 == 0:
                event_time = datetime.now(IST).replace(tzinfo=None) - timedelta(hours=3)

        else:
            customer_id = random_customer_id()

        insert_order(
            client_id=client_id,
            customer_id=customer_id,
            amount=amount,
            status=status,
            created_at=event_time,
            updated_at=event_time,
        )


@app.route("/", methods=["GET"])
def home():
    return render_template_string(HTML, message=None)


@app.route("/generate", methods=["POST"])
def generate():
    client_id = request.form["client_id"]
    records = int(request.form["records"])
    scenario = request.form["scenario"]

    generate_orders(client_id, records, scenario)

    return render_template_string(
        HTML,
        message=f"Generated {records} orders for {client_id} using scenario: {scenario}",
    )


@app.route("/generate_window", methods=["POST"])
def generate_window():
    client_id = request.form["client_id"]
    records = int(request.form["records"])
    event_date = request.form["event_date"]
    event_hour = int(request.form["event_hour"])

    base_time = datetime.strptime(
        f"{event_date} {event_hour}:00:00",
        "%Y-%m-%d %H:%M:%S",
    )

    generate_orders(client_id, records, "normal", base_time=base_time)

    return render_template_string(
        HTML,
        message=f"Generated {records} orders for {client_id} at {base_time}",
    )


if __name__ == "__main__":
    app.run(debug=True, port=5005)
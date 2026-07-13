from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from operators.delta_loader_operator import delta_loader_operator


DEFAULT_ARGS = {
    "owner": "datawatch",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="Client1_Delta_DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 6, 12),
    schedule="@hourly",
    catchup=True,
    max_active_runs=1
) as dag:

    delta_loader = PythonOperator(
        task_id="delta_loader",

        python_callable=delta_loader_operator,

        op_kwargs={
            "config": {
                "aws_endpoint": "http://localhost:4566",
                "glue_job_name": "client1_delta_loader",
                "max_threads": 3,

                "input_prefix": "s3://rawzone",
                "delta_path": "s3://delta/client1",
                "schema_path":"s3://delta/client1",

                "db_details": {
                    "host": "localhost",
                    "database": "ecommerceapp",
                    "user": "postgres",
                    "password": "root",
                    "port": 5432
                },

                "collections_config": {
                    "orders": {
                        "microservice": "microservice1",
                        "merge_key": "key"
                    },
                    "customers": {
                        "microservice": "microservice2",
                        "merge_key": "key"
                    },
                    "products": {
                        "microservice": "microservice1",
                        "merge_key": "key"
                    }
                }
            }
            
        }
    )
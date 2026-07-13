import json
import subprocess


def delta_loader_operator(config, **context):

    window_start = context["data_interval_start"].strftime("%Y-%m-%d %H:%M:%S")
    window_end = context["data_interval_end"].strftime("%Y-%m-%d %H:%M:%S")

    cmd = [
        "python",
        "/opt/airflow/jobs/glue_delta_loader.py",

        "--config",
        json.dumps(config),

        "--window_start",
        window_start,

        "--window_end",
        window_end
    ]

    print("Executing:")
    print(" ".join(cmd))

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True
    )

    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise Exception("Delta Loader Failed")
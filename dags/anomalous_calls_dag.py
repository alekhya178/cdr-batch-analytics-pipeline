from datetime import datetime, timedelta
# pyrefly: ignore [missing-import]
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'anomalous_call_detection_dag',
    default_args=default_args,
    description='Trigger PySpark Anomalous Call Detection job',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'telecom'],
) as dag:

    submit_spark_job = BashOperator(
        task_id='submit_anomalous_calls_job',
        bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /jobs/anomalous_calls.py \
            --input /data/cdr_data.csv \
            --output-dir /output/anomalous_call_detection \
            --run-id "{{ dag_run.conf.get('run_id', 'default_run') }}" \
            --num-partitions 16
        """,
    )

    submit_spark_job

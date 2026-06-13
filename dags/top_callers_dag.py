from datetime import datetime, timedelta
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
    'top_callers_by_spend_dag',
    default_args=default_args,
    description='Trigger PySpark Top 100 Callers job',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'telecom'],
) as dag:

    submit_spark_job = BashOperator(
        task_id='submit_top_callers_job',
        bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /jobs/top_callers.py \
            --input /data/cdr_data.csv \
            --output-dir /output/top_callers_by_spend \
            --run-id "{{ dag_run.conf.get('run_id', run_id) }}"
        """,
    )

    submit_spark_job

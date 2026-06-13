from datetime import datetime, timedelta
from airflow import DAG  # pyrefly: ignore [missing-import]
from airflow.operators.bash import BashOperator  # pyrefly: ignore [missing-import]

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
    'revenue_reconciliation_dag',
    default_args=default_args,
    description='Trigger PySpark Revenue Reconciliation job',
    schedule_interval=None,
    catchup=False,
    tags=['spark', 'telecom'],
) as dag:

    submit_spark_job = BashOperator(
        task_id='submit_revenue_recon_job',
        bash_command="""
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            /jobs/revenue_recon.py \
            --input /data/cdr_data.csv \
            --output-dir /output/revenue_reconciliation \
            --run-id "{{ dag_run.conf.get('run_id', 'default_run') }}"
        """,
    )

    submit_spark_job

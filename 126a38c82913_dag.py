from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta

# Define params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/imobass000@gmail.com/DataCleaning',
}

# Define params for Run Now Operator
notebook_params = {
    "Variable": 5
}

default_args = {
    'owner': '126a38c82913',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG('126a38c82913_dag',
         start_date=datetime(2024, 7, 16),
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args
         ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run_data_cleaning',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )

    opr_run_now = DatabricksRunNowOperator(
        task_id='run_now_kinesis_stream',
        databricks_conn_id='databricks_default',
        job_id='your_databricks_job_id',
        notebook_params=notebook_params
    )

    opr_submit_run >> opr_run_now

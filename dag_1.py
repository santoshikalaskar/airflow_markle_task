from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from preprocess_creative_json import process_json_file
from preprocess_creative_csv import process_csv_file

default_args = {
    "owner":"airflow",
    "depends_on_past" : False,
    "start_date": datetime(2022,1,15),
    'retries': 0
}

dag = DAG(
    dag_id="dag-2",
    default_args=default_args,
    catchup= False,
    schedule_interval='@daily')

check_creatives_file = BashOperator(
        task_id="check_creatives_file",
        dag=dag,
        bash_command="shasum /home/santoshi/airflow/dags/dataset/creatives.json",
        retries = 2,
        retry_delay=timedelta(seconds=15))

check_campaigns_file = BashOperator(
        task_id="check_campaigns_file",
        dag=dag,
        bash_command="shasum /home/santoshi/airflow/dags/dataset/campaigns.csv",
        retries = 2,
        retry_delay=timedelta(seconds=15))

process_json = PythonOperator(
        task_id = "process_json",
        dag=dag,
        python_callable = process_json_file
        )

process_csv = PythonOperator(
        task_id = "process_csv",
        dag=dag,
        python_callable = process_csv_file
        )

check_campaigns_file >> process_csv
check_creatives_file >> process_json
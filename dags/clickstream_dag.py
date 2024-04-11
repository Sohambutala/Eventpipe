from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from faker import Faker
from faker_clickstream import ClickstreamProvider
import os
import json

default_args = {
    'owner': 'Soham',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def generate_clickstream():
    # Specify the directory where you want to save your log file
    log_directory = "/opt/airflow/streamlogs"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_file_name = f"clickstream_log_{timestamp}.log"
    log_file_path = os.path.join(log_directory, log_file_name)

    fake = Faker()
    fake.add_provider(ClickstreamProvider)
    events = fake.session_clickstream(rand_session_max_size=50)

    with open(log_file_path, 'a') as log_file:  # Open the log file in append mode
        for e in events:
            log_file.write(json.dumps(e) + '\n')  # Write each event as a new line in the log file

with DAG(
    'clickstream_generation',
    default_args=default_args,
    description='Generate clickstream data every minute',
    schedule_interval='*/1 * * * *',  # Every minute
    ) as dag:
    
    generate_clickstream_task = PythonOperator(
        task_id='generate_clickstream',
        python_callable=generate_clickstream
    )

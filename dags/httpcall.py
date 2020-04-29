from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datacleaner import data_cleaner
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.email_operator import EmailOperator
from airflow.hooks.http_hook import HttpHook
import requests
import json

default_args = {
    'owner': 'Airflow',
	'depends_on_past': False,
    'start_date': datetime(2020, 3, 25),
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}
def create_process_for_matched_data(ds, **kwargs):
    obj = { "name": "Testing 5", "processDefinitionKey": "testingprocess"}
    headers = {'content-type': 'application/json','Authorization': 'Basic YWRtaW5AYXBwLmFjdGl2aXRpLmNvbTphZG1pbg==' }
    r =requests.post("http://111.93.191.82:9595/activiti-app/api/enterprise/process-instances",json.dumps(obj),
    headers= headers,verify=False)
    print(r)


with DAG('HTTP_CALL',default_args=default_args,schedule_interval='@daily') as dag:
    t1 = PythonOperator(task_id='create-process-for-customer', python_callable=create_process_for_matched_data, on_failure_callback=create_process_for_matched_data,
    provide_context=True, dag=dag)
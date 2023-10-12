from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
from etl import extract, transform, extract_sql, transform_sql, merge, load, store

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 11),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements
)

def etl_workshop():

    
    @task
    def extract_task ():
        return extract(),

    @task
    def transform_task (json_data):
        return transform(json_data)

    @task
    def extract_db():
        return extract_sql()
    
    @task
    def transform_db(json_data):

        return transform_sql(json_data)
    
    @task
    def merge_all(data1, data2):

        return merge(data1, data2)
    
    @task
    def load_all(json_data):

        return load(json_data)
    
    @task
    def store_all(json_data):

        return store(json_data)


    data= extract_task()
    data=transform_task(data)

    data2 = extract_db()
    data2=transform_db(data2)

    data_f=merge_all(data, data2)

    data_load= load_all(data_f)

    store_all(data_load)

    

workflow_api_etl_dag = etl_workshop()

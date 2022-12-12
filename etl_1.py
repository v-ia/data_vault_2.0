from airflow import DAG
from airflow.operators.python import task
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta

default_args = {
    'owner': 'Vadim',
    'depends_on_past': False,
    'start_date': datetime(2022, 12, 11),
    'schedule_interval': '@daily',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(hours=1)
}

with DAG(
    dag_id='etl1',
    description='Data extraction from API and loading to STG',
    tags=['stg'],
    default_args=default_args
) as dag:

    @task(task_id='to_stg')
    def etl1():
        import psycopg2
        import requests

        connection = BaseHook.get_connection('postgres_stg')
        posts = requests.get('https://jsonplaceholder.typicode.com/posts/')
        json_posts = posts.json()
        with psycopg2.connect(
                host=connection.host,
                port=connection.port,
                user=connection.login,
                password=connection.password,
                dbname=connection.schema
        ) as conn:
            with conn.cursor() as cur:
                for post in json_posts:
                    cur.execute("INSERT INTO posts (id, user_id, title, body, load_date, rec_source) VALUES"
                                "(%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                (post['id'],
                                 post['userId'],
                                 post['title'],
                                 post['body'],
                                 datetime.now().date(),
                                 'jsonplaceholder.typicode.com/posts/')
                                )
    etl1()

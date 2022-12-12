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
    dag_id='etl2',
    description='Data transformation and loading to DDS',
    tags=['dds', 'test_task'],
    default_args=default_args
) as dag:

    @task(task_id='to_dds')
    def etl2():
        import psycopg2
        import hashlib

        connection_stg = BaseHook.get_connection('postgres_stg')
        with psycopg2.connect(
                host=connection_stg.host,
                port=connection_stg.port,
                user=connection_stg.login,
                password=connection_stg.password,
                dbname=connection_stg.schema
        ) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, user_id, title, body, rec_source FROM posts;")
                posts = cur.fetchall()

        if posts:
            connection_dds = BaseHook.get_connection('postgres_dds')
            with psycopg2.connect(
                    host=connection_dds.host,
                    port=connection_dds.port,
                    user=connection_dds.login,
                    password=connection_dds.password,
                    dbname=connection_dds.schema
            ) as conn:
                with conn.cursor() as cur:
                    for post in posts:
                        post_bk, user_bk, post_title, post_body, rec_source = post
                        load_date = datetime.now().date()

                        post_key_hash = hashlib.md5(str(post_bk).encode('utf-8')).hexdigest()
                        cur.execute(
                            "INSERT INTO hub_post (post_key, post_bk, load_date, rec_source) VALUES(%s, %s, %s, %s) "
                            "ON CONFLICT DO NOTHING",
                            (post_key_hash, post_bk, load_date, rec_source))

                        user_key_hash = hashlib.md5(str(user_bk).encode('utf-8')).hexdigest()
                        cur.execute(
                            "INSERT INTO hub_user (user_key, user_bk, load_date, rec_source) VALUES(%s, %s, %s, %s) "
                            "ON CONFLICT DO NOTHING",
                            (user_key_hash, user_bk, load_date, rec_source))

                        written_by_key_hash = hashlib.md5(f'{post_bk}^{user_bk}'.encode('utf-8')).hexdigest()
                        cur.execute(
                            "INSERT INTO link_written_by (written_by_key, user_key, post_key, load_date, rec_source) "
                            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (written_by_key_hash, user_key_hash, post_key_hash, load_date, rec_source))

                        hash_diff = hashlib.md5(f'{post_title}^{post_body}'.encode('utf-8')).hexdigest()
                        cur.execute("INSERT INTO sat_post_content"
                                    "(post_key, hash_diff, load_date, rec_source, post_title, post_body) VALUES"
                                    "(%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                                    (post_key_hash, hash_diff, load_date, rec_source, post_title, post_body))
    etl2()

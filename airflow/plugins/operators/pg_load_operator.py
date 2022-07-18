from airflow.models import BaseOperator, DAG, TaskInstance
from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook
from datetime import datetime
import json
import os
from pathlib import Path
from airflow.hooks.postgres_hook import PostgresHook

class PostgresLoadOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
        conn_id=None,
        connection=None,
        *args, **kwargs
        ):
        super().__init__(*args, **kwargs) #*args, **kwargs
        pg_hook = PostgresHook(
            postgres_conn_id='postgres_default',
            schema='boticario'
        )
        self.conn_id = conn_id
        self.connection = pg_hook.get_conn()


    def read_file(self, src):
        with open(src, 'r') as file:
            content = file.readlines()
            return content


    def load_file_to_db(self, json_list):
        cursor = self.connection.cursor()
        sql = 'INSERT INTO boticario.tb_tweets (usuario, tweet) VALUES {values}'
        for row in json_list:
            json_row = json.loads(row)
            user_id = json_row["username"]
            tweet_msg = str(json_row["text"]).replace("'", "")
            values = f"('{user_id}', '{tweet_msg}')"
            cursor.execute(sql.format(values=values))
        self.connection.commit()


    def execute(self, context):
        silver_path = '/home/priffert/datapipe/datalake/silver/boticario_twetts'
        file_list = os.listdir(silver_path)
        file_list = [file for file in file_list if file.endswith('.json')]
        for file in file_list:
            json_content = self.read_file(os.path.join(silver_path, file))
            self.load_file_to_db(json_content)


if __name__ == '__main__':
    with DAG(dag_id='PostgresTest', start_date=datetime.now()) as dag:
        to = PostgresLoadOperator(
            task_id='test_run_postgres'
        )
        # ti = TaskInstance(task=to, execution_date=datetime.now())
        # to.execute(ti.get_template_context())
        to.execute()
        
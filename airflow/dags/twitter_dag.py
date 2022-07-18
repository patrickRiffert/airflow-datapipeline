from airflow.models import DAG
from datetime import datetime
from os.path import join
from operators.twitter_operator import TwitterOperator
from operators.pg_load_operator import PostgresLoadOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


# Linha de produtos com mais vendas no mês 12 de 2019
def get_best_seller():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_default',
        schema='boticario'
    )
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute('''
    select "LINHA" from (
        select *,
            row_number () over (order by "total_vendas" desc) as "row_rank"
        from boticario.tb_vendas_linha_ano_mes
        where "ano" = 2019
            and "mes" = 12
    ) as tb
    where tb.row_rank = 1
    ''')
    best_seller = cursor.fetchall()
    # print(best_seller[0][0].lower())
    # print(type(best_seller[0][0]))
    return best_seller[0][0].lower()

linha = get_best_seller()


with DAG(dag_id='twitter_dag', start_date=datetime.now(), schedule_interval='0 10 * * *', max_active_runs=1) as dag:
    twitter_operator = TwitterOperator(
        task_id='twitter_boticario',
        query=f'Boticário {linha}',
        file_path=join(
                '/home/priffert/datapipe/datalake/bronze',
                'boticario_twetts',
                f'dt_extraction={datetime.now().strftime("%Y-%m-%d")}',
                f'tweets_{datetime.now().strftime("%Y-%m-%d")}.json'
            )
    )

    twitter_transform = SparkSubmitOperator(
        task_id='transform_twitter_boticario',
        application='/home/priffert/datapipe/spark/transform.py',
        name='twitter_tranform',
        application_args=[
            '--src',
            f'/home/priffert/datapipe/datalake/bronze/boticario_twetts/dt_extraction={datetime.now().strftime("%Y-%m-%d")}',
            '--dest',
            '/home/priffert/datapipe/datalake/silver/boticario_twetts'
        ]
    )

    load_tweets_to_db = PostgresLoadOperator(
        task_id='load_tweets_to_db'
    )

    twitter_operator >> twitter_transform >> load_tweets_to_db
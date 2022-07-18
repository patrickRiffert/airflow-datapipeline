from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import json

queries = [
# Tabela1: Consolidado de vendas por ano e mês
'''
drop table if exists boticario.tb_vendas_ano_mes;
create table boticario.tb_vendas_ano_mes as (
	select
		extract(year from to_date("dt_venda", 'DD/MM/YYYY')) as "ano"
		,extract(month from to_date("dt_venda", 'DD/MM/YYYY')) as "mes"
		,sum("QTD_VENDA")
	from boticario.tb_vendas_dia
	group by
		extract(year from to_date("dt_venda", 'DD/MM/YYYY'))
		,extract(month from to_date("dt_venda", 'DD/MM/YYYY'))
);
'''
,
# Tabela2: Consolidado de vendas por marca e linha
'''
drop table if exists boticario.tb_vendas_marca_linha;
create table boticario.tb_vendas_marca_linha as (
	select "MARCA", "LINHA", sum("QTD_VENDA") as "total_vendas"
	from boticario.tb_vendas_dia
	group by "MARCA", "LINHA"
);
'''
,
# Tabela3: Consolidado de vendas por marca, ano e mês
'''
drop table if exists boticario.tb_vendas_marca_ano_mes ;
create table boticario.tb_vendas_marca_ano_mes as (
	select 
		"MARCA"
		,extract(year from to_date("dt_venda", 'DD/MM/YYYY')) as "ano"
		,extract(month from to_date("dt_venda", 'DD/MM/YYYY')) as "mes"
		,sum("QTD_VENDA") as "total_vendas"
	from boticario.tb_vendas_dia
	group by
		"MARCA"
		,extract(year from to_date("dt_venda", 'DD/MM/YYYY'))
		,extract(month from to_date("dt_venda", 'DD/MM/YYYY'))
);
'''
,
# Tabela4: Consolidado de vendas por linha, ano e mês
'''
drop table if exists boticario.tb_vendas_linha_ano_mes;
create table boticario.tb_vendas_linha_ano_mes as (
	select 
		"LINHA"
		,extract(year from to_date("dt_venda", 'DD/MM/YYYY')) as "ano"
		,extract(month from to_date("dt_venda", 'DD/MM/YYYY')) as "mes"
		,sum("QTD_VENDA") as "total_vendas"
	from boticario.tb_vendas_dia
	group by
		"LINHA"
		,extract(year from to_date("dt_venda", 'DD/MM/YYYY'))
		,extract(month from to_date("dt_venda", 'DD/MM/YYYY'))
);
'''
]



def get_db_connection():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_default',
        schema='boticario'
    )
    return pg_hook.get_conn()


def execute_sql(query_list, cursor):
    for query in query_list:
        cursor.execute(query)


def create_views():
    conn = get_db_connection()
    cursor = conn.cursor()
    execute_sql(query_list=queries, cursor=cursor)
    conn.commit()



with DAG(
    dag_id='create_views_vendas_dag',
    schedule_interval='@daily',
    start_date=datetime.now(),
    catchup=False
) as dag:

    load_to_postgres = PythonOperator(
        task_id='create_views_vendas',
        python_callable=create_views,
    )

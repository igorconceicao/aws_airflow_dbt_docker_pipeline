#!pip install airflow  Tem que instalar a Bibioteca aiflow

from datetime import datetime, timedelta 
from airflow.decorators import dag, task import dag, task 
from airflow.providers.postgres.hooks.postgres import PostgresHook 
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook 



default_args = { #Criação de parametro que serão utilizados na DAG
    'owner': 'airflow', # proprietario da DAG
    'depends_on_past': False, #Sem dependecias de passado
    'start_date': datetime(2026, 4, 19),  #Data iniciar
    'email_on_failure': False, #Caso de falha enviara um e-mail
    'email_on_retry': False,   #Reenviar e-mail caso falha.
    'retries': 0,
    'retry_delay': timedelta(minutes=1),  #Será o tempo que que ele vai aguardar até reenviar o e-mail de novo.
}
 
@dag( #vamos utilizar o "decoration" da DAG. Ouseja, ele vai trazer funcionalidade a uma função
    dag_id='postgres_to_snowflake',
    default_args=default_args, #Var
    description='Load data incrementally from Postgres to Snowflake', #Descrição da Dag
    schedule=timedelta(days=1), #Informará de quanto em quanto tempo está DAG será executada.
    catchup=False #É um parametro que ele reexecuta todas as execuções que falharam novamente
)
def postgres_to_snowflake_etl(): #Declara a função
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']
 
    for table_name in table_names:
        @task(task_id=f'get_max_id_{table_name}')
        def get_max_primary_key(table_name: str): #Os dados de conexão do snowflake e postgres serão configrados no Airflow. 
            with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as conn:#Criando a conexão com Snowflake com SnowflakeHook.
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT MAX(ID_{table_name}) FROM {table_name}")
                    max_id = cursor.fetchone()[0] # "fetchone" é pq ele somente vai retornar somente 1 valor que será o maior ID.
                    return max_id if max_id is not None else 0 #Retorna o valor de Max_id se não for null, caso contrário retorna None = 0.
 
        @task(task_id=f'load_data_{table_name}')
        def load_incremental_data(table_name: str, max_id: int): # Criação da função para carregar dados de forma incremental
            with PostgresHook(postgres_conn_id='postgres').get_conn() as pg_conn: #Criandoa conexão com Postgres com PostgresHook.
                with pg_conn.cursor() as pg_cursor:
                    primary_key = f'ID_{table_name}'
                    
                    # Está consultando o nome de todas as colunas com "information_schema.columns"
                    pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                    columns = [row[0] for row in pg_cursor.fetchall()] #Fetchall para retornar todos os registros(Nomes colunas)
                    columns_list_str = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(columns))  # Montou uma lista dos nome das colunas da tabela atual.
                    #placeholders é por uma questão de segurança, 
                    # pois ele garante que os valores seja inserido na hora da inserção, 
                    # pois os valores são substituidos por '%s' a cada posição na lista, 
                    # para evitar SQL INJECT ou ataques.


                    pg_cursor.execute(f"SELECT {columns_list_str} FROM {table_name} WHERE {primary_key} > {max_id}")
                    rows = pg_cursor.fetchall()
                    
                    with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as sf_conn:
                        with sf_conn.cursor() as sf_cursor: #Abrir o cursor
                            insert_query = f"INSERT INTO {table_name} ({columns_list_str}) VALUES ({placeholders})"
                            for row in rows: # Este Laço é para o INSERT ser inseriado a cada linha no 
                             sf_cursor.execute(insert_query, row) # snowflake que exitir no postgre 
 
        #Chamando as Taks criadas acima.
        max_id = get_max_primary_key(table_name) #No final do código exite uma última verificação do maior ID do Snowflake.
        load_incremental_data(table_name, max_id) #No final do código exite uma última verificação se 
                                                  #está ingual os nomes das tabelas maior ID do Snowflake.

#Chamandoa DAG e para fazer como Aiflow pode executar a DAG por dentro do script.
postgres_to_snowflake_etl_dag = postgres_to_snowflake_etl() #Execução da DAG para o Airflow executar. 
 

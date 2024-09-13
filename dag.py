from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='postgres_to_snowflake',
    default_args=default_args,
    description='Load data incrementally from Postgres to Snowflake',
    schedule_interval=timedelta(days=1),
    catchup=False
)
def postgres_to_snowflake_etl():
    table_names = ['veiculos', 'estados', 'cidades', 'concessionarias', 'vendedores', 'clientes', 'vendas']

    @task
    def get_max_primary_key(table_name: str):
        with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT MAX(ID_{table_name}) FROM {table_name}")
                max_id = cursor.fetchone()[0]
                return max_id if max_id is not None else 0

    @task
    def load_incremental_data(table_name: str, max_id: int):
        with PostgresHook(postgres_conn_id='postgres').get_conn() as pg_conn:
            with pg_conn.cursor() as pg_cursor:
                primary_key = f'ID_{table_name}'
                
                # Fetch columns dynamically
                pg_cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                columns = [row[0] for row in pg_cursor.fetchall()]
                columns_list_str = ', '.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
                
                # Fetch rows greater than max_id
                pg_cursor.execute(f"SELECT {columns_list_str} FROM {table_name} WHERE {primary_key} > {max_id}")
                rows = pg_cursor.fetchall()
                
                # Insert data into Snowflake
                with SnowflakeHook(snowflake_conn_id='snowflake').get_conn() as sf_conn:
                    with sf_conn.cursor() as sf_cursor:
                        insert_query = f"INSERT INTO {table_name} ({columns_list_str}) VALUES ({placeholders})"
                        for row in rows:
                            sf_cursor.execute(insert_query, row)
    
    # Setting dependencies outside the loop to avoid creating cycles
    for table_name in table_names:
        max_id = get_max_primary_key(table_name)  # Gets the maximum primary key from Snowflake
        load_incremental_data(table_name, max_id)  # Loads incremental data from Postgres to Snowflake

postgres_to_snowflake_etl_dag = postgres_to_snowflake_etl()

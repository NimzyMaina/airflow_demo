from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

def _transform_results(ti):
    stats = ti.xcom_pull(task_ids='run_analytics_query',key='return_value')
    table_name = 'Analytics'
    query = f'insert into {table_name} values'

    for row in stats:
        query += f" ({str(row)[1:-1]}),"
    
    query = query[:-1] + ';'
    print(query)
    return query
    

with DAG(dag_id='get_trip_analytics',
    schedule_interval="@daily",
    start_date=datetime(2021,1,1),
    catchup=False) as dag:

    run_analytics_query = ClickHouseOperator(
        task_id="run_analytics_query",
        database="default",
        sql="analytics.sql",
        query_id='{{ ti.dag_id }}-{{ ti.task_id }}-{{ ti.run_id }}-{{ ti.try_number }}',
        clickhouse_conn_id="click_house_01"
    )

    cleanup_sqlite_db = SqliteOperator(
        task_id="cleanup_sqlite_db",
        sqlite_conn_id='sqlite_01',
        sql="drop_table.sql"
    )

    create_sqlite_db = SqliteOperator(
        task_id="create_sqlite_db",
        sqlite_conn_id='sqlite_01',
        sql="create_table.sql"
    )


    transform_results = PythonOperator(
        task_id='transform_results',
        python_callable=_transform_results
    )


    insert_query_results = SqliteOperator(
        task_id="insert_query_results",
        sqlite_conn_id='sqlite_01',
        sql=transform_results.output
    )


cleanup_sqlite_db >> create_sqlite_db >> run_analytics_query >> transform_results >> insert_query_results
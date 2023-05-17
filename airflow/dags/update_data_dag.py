import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.get_data import update_daily_data
from src.clean_data import clean_data

PATH_DATA = "/data"

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True,
}

dag = DAG(
    dag_id='update_data_dag',
    default_args=args,
    schedule_interval= '@daily',
	catchup=False,
)

task1 = PythonOperator(
    task_id='update_daily_data',
    python_callable=update_daily_data,
    dag=dag,
    op_kwargs={'path_data': PATH_DATA}
)

task2 = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag,
    op_kwargs={'path_data': PATH_DATA}
)

task1 >> task2
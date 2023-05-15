import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.get_data import get_init_data

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True,
}

dag = DAG(
    dag_id='init_data_dag',
    default_args=args,
    schedule_interval= '@once',
	catchup=False,
)

task = PythonOperator(
    task_id='get_init_data',
    python_callable=get_init_data,
    dag=dag
)

task
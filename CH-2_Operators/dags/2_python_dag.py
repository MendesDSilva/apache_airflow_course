from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator

def first_task_function():
    return "Hello World!"

def second_task_function():
    return "Hello World!"


@dag
def python_dag(dag_id="python_dag"):

    first_task = PythonOperator(
        task_id="first_task",
        python_callable=first_task_function
    )

    second_task = PythonOperator(
        task_id="second_task",
        python_callable=second_task_function

    )
    # Define tasks dependencies 
    first_task >> second_task

pyhton_dag_instance = python_dag()
from airflow.sdk import dag,task
from child_dag_first import child_dag_first_dag
from child_dag_second import child_dag_second_dag



@dag 
def parent_dag():

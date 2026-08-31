from airflow.sdk import dag, task

@dag
def status_dependency_dag():

    @task.python
    def task_a():
        print("Executing Task A")
        return "Task A completed"


    @task.python
    def task_b(ti):
        task_a_result = ti.xcom_pull()
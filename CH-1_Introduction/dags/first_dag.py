from airflow.sdk import dag, task

@dag(dag_id="first_dag")
def first_dag():

    @task
    def task_1(task_id="task 1"):
        print("This is task 1")


    @task
    def task_2(task_id="task 2"):
        print("This is task 2")

    @task
    def task_3(task_id="task 3"):
        print("this is task3")

    # Define the task dependencies
    t1 = task_1()
    t2 = task_2()
    t3 = task_3()

    t1 >> t2 >> t3 # This means task 1 will run before task 2

# To run the DAG, we need to create an instance of it

first_dag_instance  = first_dag()
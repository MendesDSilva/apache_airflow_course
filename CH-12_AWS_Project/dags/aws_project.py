import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.bronze_layer import BronzeLayer
from utils.silver_layer import SilverLayer
from utils.gold_layer import GoldLayer
from airflow.sdk import task, dag
from datetime import timedelta, datetime



# ----------------------------------- Code -----------------------------------------------#


@dag(
    schedule='@daily',
    is_paused_upon_creation=False
)
def aws_project():

    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def extract_load():

        #Fetching datra from API and loading it to S3 using loop
        urls = ["https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/bookings.csv",
                "https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/passengers.csv",
                "https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/airports.csv"]

        obj = BronzeLayer()

        folder_name = datetime.now().strftime("%Y-%m-%d")
        
        for url in urls:
            fetched_data = obj.ingest_data_api(url)
            obj.put_data_s3("airflow-aws-course-bucket",f"bronze/{folder_name}/{url.split('/')[-1]}",fetched_data)
        
        return folder_name

    # Triggering the spark job in AWS glue to transform the data and load it to s3
    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def transform_load_s3(ti):
        # Fetching the last load date
        last_load_date = ti.xcom_pull(task_ids='extract_load',key = 'return_value')

        obj = SilverLayer()

        # Truggering the spark Job using glue to transform data and load it to S3
        job_run_id = obj.trigger_spark_job("silver_layer",last_load_date)
        print(f"Glue Job triggered with JobRunId: {job_run_id}")



     # Triggering the spark job in AWS glue to transform the data parquet and load it to s3
    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def transform_load_s3_parquet(ti):
        # Fetching the last load date
        last_load_date = ti.xcom_pull(task_ids='extract_load',key = 'return_value')
    
        obj = SilverLayer()
    
        # Truggering the spark Job using glue to transform data and load it to S3
        job_run_id = obj.trigger_spark_job("silver_layer_athena",last_load_date)
        print(f"Glue Job triggered with JobRunId: {job_run_id}")
    

    # Task to trigger Glue Crawler
    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def trigger_crawler():
        
        obj = SilverLayer()
        
        # Truggering the spark Job using glue to transform data and load it to S3
        crawler_response= obj.trigger_crawler("crawler_silver")
        print(crawler_response)


    # Task to trigger the databricks job to transform the data and load it to the Gold layer
    @task.python(retries=3, retry_delay=timedelta(seconds=5))
    def trigger_databricks_job():
        obj = GoldLayer()
        response = obj.trigger_databricks_job(job_id=750378970164515)
        print(response)

    
    # Task dependency
    extract_load() >> [transform_load_s3(),transform_load_s3_parquet()] >> trigger_crawler() >> trigger_databricks_job()

    

aws_project_dag = aws_project()
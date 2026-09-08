import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__)),'..')
from utils.bronze_layer import BronzeLayer
from airflow.sdk import task, dag
from datetime import timedelta


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

        for url in urls:
            fetched_data = obj.ingest_data_api(url)
            obj.put_data_s3("airflow-aws-course-bucket",f"bronze/{url.split('/')[-1]}", fetched_data)

    # Task dependency
    extract_load()

aws_project_dag = aws_project()
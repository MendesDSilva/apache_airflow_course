from airflow.sdk import dag, task
import requests
import pandas as pd
import os
from sqlalchemy import create_engine, text

@dag
def etl_pipeline():

    # Defining data format
    @task
    def timestamp():
        from datetime import datetime
        return datetime.now().isoformat()

   
    # Reading and writing the data to tmp layer into the Docker
    @task
    def extract(ti):

        # Fetching timestamp from previous task
        timestamp = ti.xcom_pull(task_ids="timestamp", key="return_value")
        
        url = "http://fastapi:8000/fetch_data"
        response = requests.get(url)
        data = response.json().get("data", [])

        # Creating staging directory if it doesn't exist
        os.makedirs("/tmp/raw", exist_ok=True)


        # Writing data to staging layer ("/tmp/raw/data.csv")
        with open (f"/tmp/raw/data_{timestamp}.csv", "w") as f:
            f.write("id,name,Age\n") # Writing header
            for item in data:
                f.write(f"{item['id']},{item['name']},{item['Age']}\n")

        return "Data extracted and stored in staging layer."        
    

    @task
    def transform(ti):
        # Fetching timestamp from previous task
        timestamp = ti.xcom_pull(task_ids ="timestamp", key="return_value")

        # Reading darta from staging layer 
        df = pd.read_csv(f"/tmp/raw/data_{timestamp}.csv")

        # Transforming data()e.g, adding a new column "age_group"
        df["age_group"] = df['Age'].apply(lambda x: "Young" if x < 30 else "Adult")

        # Creating transformed data directory if it doesn't exist
        os.makedirs("/tmp/transformed", exist_ok=True)

        #Writing transformed data to transformed layer
        df.to_csv(f"/tmp/transformed/data_transformed_{timestamp}.csv", index=False)



    @task
    def crate_tables():
        query = """
        CREATE TABLE IF NOT EXISTS employees(
            id INT PRIMARY KEY,
            name VARCHAR(255),
            age INT,
            age_group VARCHAR(50)
        );
        """
        conn = create_engine("postgresql://airflow:airflow@postgres:5432/airflow").connect()

        with conn.begin() as transaction:
            try:
                conn.execute(text(query))
            except Exception as e:
                transaction.rollback()
                raise e
            else:
                transaction.commit()

    @task
    def load(ti):

        # Fetching timestamp from previous task
        timestamp = ti.xcom_pull(task_ids = "timestamp", key = "return_value")

        # Reading transformed data from transformed layer
        df = pd.read_csv(f"/tmp/transformed/data_transformed_{timestamp}.csv")

        # Loading data to Postgres
        engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")
        df.to_sql("employees", con=engine, if_exists="append",index=False)
        
        engine.dispose()

    # Define the task dependencies
    timestamp() >> extract() >> transform() >> load()

etl_pipeline()


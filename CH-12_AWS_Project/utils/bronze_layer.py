# URL For the API:
# https://github.com/anshlambagit/ApacheAirflow/blob/main/bookings.csv

class BronzeLayer:

    def __init__(self):
        pass

    def ingest_data_api(self,url):
        import pandas as pd
        import requests
        from io import StringIO

        response= requests.get(url)

        if response.status_code == 200:
            data = response.text

            df = pd.read_csv(StringIO(data))

            # Converting the  Dataframe to csv in memory using StringIO
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)

            # Print the CSV content
            print(csv_buffer.getvalue())
            return csv_buffer.getvalue()

        else:
            print(f"Failed to fetch data. Status code: {response.status_code}")


    def put_data_s3(self, bucket_name, object_key, data):

        from dotenv import load_dotenv
        import os
        import boto3

        load_dotenv()

        aws_access_key_id = os.environ.get("aws_access_key_id")
        aws_secret_access_key = os.environ.get("aws_secret_access_key")

        s3_client = boto3.client(
            's3',
            region_name = 'us-east-1',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )

        # Upload the data to s3
        s3_client.put_object(
            Bucket = bucket_name,
            Key=object_key,
            Body=data
        )
        return f"Data uploaded to s3 bucket '{bucket_name}' with object key '{object_key}'"

if __name__=="__main__":

    obj = BronzeLayer()

    data = obj.ingest_data_api("https://raw.githubusercontent.com/anshlambagit/ApacheAirflow/refs/heads/main/bookings.csv")
    obj.put_data_s3("airflow-aws-course-bucket","bronze/bookings.csv",data)

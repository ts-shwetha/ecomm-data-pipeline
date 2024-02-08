"""
Data Collection:

Website User Event Tracking: Use tools like Google Analytics, Mixpanel, or your own custom tracking system to collect user event data from the website.
Mobile User Event Tracking: Utilize tools such as Firebase Analytics or Amplitude to collect user event data from mobile devices.
OLTP Data from PostgreSQL: Set up database connectors or APIs to extract relevant data from your PostgreSQL database. This could include information on orders, customers, products, etc.
Third-party Data via Webhooks: Implement webhook listeners to receive data from third-party systems. Parse and process the incoming data to extract relevant information.


Data Transformation:

Cleanse and preprocess the raw data to handle missing values, outliers, and inconsistencies.
Perform data transformations such as joining, aggregating, and enriching datasets to prepare them for analytics.
Convert data into a unified format/schema to facilitate integration and analysis.


Data Storage:

Choose appropriate storage solutions based on your requirements and scale. This could include relational databases, data lakes (e.g., Amazon S3, Azure Data Lake Storage), or data warehouses (e.g., Amazon Redshift, Google BigQuery).
Store raw, processed, and aggregated data separately to maintain data lineage and enable traceability.

Data Integration:

Integrate data from different sources using ETL (Extract, Transform, Load) processes or ELT (Extract, Load, Transform) processes depending on the volume and complexity of data.
Use orchestration tools like Apache Airflow or AWS Step Functions to schedule and automate data integration workflows.
Ensure data consistency and integrity across integrated datasets.

Data Analysis:

Perform exploratory data analysis (EDA) to gain insights into customer behavior, sales trends, product performance, etc.
Utilize business intelligence (BI) tools such as Tableau, Power BI, or Looker for interactive visualizations and dashboards.
Implement machine learning models for predictive analytics, recommendation systems, or customer segmentation.


Data Visualization and Reporting:

Create interactive dashboards and reports to visualize key metrics and KPIs.
Share insights with stakeholders through scheduled reports or real-time dashboards.
Incorporate feedback from stakeholders to refine analytics and reporting processes.

Monitoring and Maintenance:

Implement monitoring and alerting systems to track pipeline performance, data quality issues, and anomalies.
Conduct regular maintenance activities such as updating schemas, optimizing queries, and scaling infrastructure to accommodate growing data volumes.
Iterate on the pipeline based on evolving business requirements and feedback from users.

"""

import requests
import boto3
from json import dumps
import psycopg2
from datetime import datetime

current_datetime = datetime.now()
current_time_stamp = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

def fetch_website_user_data(api_url):
    # Make a GET request to the website user event tracking API
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data from {api_url}. Status code: {response.status_code}")
        return None
def fetch_mobile_user_data(file_path, file_name, bucket_name):
    with open(f'{file_path}/{file_name}', 'r') as f:
        data = f.readlines()
    loaded_path = f'raw_layer/mobile/{file_name}'
    upload_to_s3(dumps(data), bucket_name, loaded_path)

def upload_to_s3(data, bucket_name, file_name):
    # Initialize S3 client
    s3 = boto3.client('s3')

    try:
        # Upload data to S3
        s3.put_object(Bucket=bucket_name, Key=file_name, Body=data)
        print(f"Data uploaded successfully to s3://{bucket_name}/{file_name}")
    except Exception as e:
        print(f"Failed to upload data to S3: {e}")

def webhook_listener(bucket_name):
    # Check if the request contains JSON data
    if requests.headers['Content-Type'] == 'application/json':
        # Get the JSON data from the request
        webhook_data = requests.json
        print("Received webhook data:")
        print(webhook_data)
        loaded_path = f'raw_layer/webhook/webhook_{current_time_stamp}.json'
        upload_to_s3(dumps(webhook_data), bucket_name, loaded_path)

        # Process the webhook data as needed (e.g., store it, perform further actions)

        return 'Webhook received successfully', 200
    else:
        return 'Unsupported Media Type', 415

def fetch_data_from_postgres(host, user, password, database):
    try:
        conn = psycopg2.connect(
            dbname="your_db_name",
            user="your_db_user",
            password="your_db_password",
            host="your_db_host",
            port="your_db_port"
        )
        # Create a cursor
        cur = conn.cursor()

        # Execute a query to fetch data
        cur.execute("SELECT * FROM your_table")

        # Fetch all rows
        rows = cur.fetchall()

        # Close cursor and connection
        cur.close()
        conn.close()

        return rows
    except psycopg2.Error as e:
        print("Error fetching data from PostgreSQL:", e)
        return None

def main():
    # API URL for website user event tracking data
    api_url = "https://example.com/api/user_events"

    # AWS S3 bucket details
    bucket_name = 'data-lake-devb'
    website_file_name = 'user_event_data.json'
    mobile_file_path = '/user/mobile/event_tracking'
    mobile_file_name = 'mobile_event_data.csv'
    host = 'postgres server'
    user = 'user_details'
    password = 'password'
    db_name = 'database_details'
    

    # Fetch data from the website user event tracking API
    user_event_data = fetch_website_user_data(api_url)
    if user_event_data:
        # Convert data to JSON string
        json_data = dumps(user_event_data)

        # Upload data to AWS S3
        loaded_path = f'raw_layer/website/{website_file_name}'
        upload_to_s3(json_data, bucket_name, loaded_path)
    fetch_mobile_user_data(mobile_file_path, mobile_file_name, bucket_name)
    webhook_listener(bucket_name)
    data_content = fetch_data_from_postgres(host, user, password, db_name)
    if data_content is not None:
        loaded_path = f'raw_layer/oltp/oltp_{current_time_stamp}.csv'
        upload_to_s3(dumps(data_content), bucket_name, loaded_path)
    

if __name__ == "__main__":
    main()


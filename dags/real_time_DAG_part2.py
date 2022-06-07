import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator


'''''
Real time data pipeline DAG
The DAG invoke 2 lambda function which extract, transform and load data from 2 api's

Pseudo code
- Start DAG on a daily basis
- Initiate lambda function: Thermal anomalies
- Initiate lambda function: Weather information
'''''

## Create DAG
dag_realTime = DAG(
    dag_id='Real_time_Wildfires_monitor',
    start_date = (datetime.datetime.now() - datetime.timedelta(days=1)),
    schedule_interval="@daily"
)

'''''
Functions
'''''
def start():
    logging.info('Starting the DAG')


## functions to invoke the aws lambdas
def lambda_ThAnomalies_invoke():
    ## Extract aws credentials from airflow admin connections
    aws_hook = AwsBaseHook(aws_conn_id='lambda_ThAnomalies', client_type='lambda')
    credentials = aws_hook.get_credentials()
    logging.info(credentials)
    ## Create a connection to aws using the credentials
    lambda_client = boto3.client('lambda',
                            region_name='us-east-1',
                            aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key,
                            aws_session_token=credentials.token)
    ## Invoke lambda function
    response_1 = lambda_client.invoke(FunctionName='lambda_ThAnomalies',InvocationType='RequestResponse')
    print ('Response--->' , response_1)

def lambda_weather_invoke():
    aws_hook = AwsBaseHook(aws_conn_id='lambda_weather', client_type='lambda')
    credentials = aws_hook.get_credentials()
    logging.info(credentials)
    lambda_client = boto3.client('lambda',
                            region_name='us-east-1',
                            aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key,
                                aws_session_token=credentials.token)
    response_1 = lambda_client.invoke(FunctionName='lambda_realtime',InvocationType='RequestResponse')
    print ('Response--->' , response_1)


def lambda_merge_invoke():
    aws_hook = AwsBaseHook(aws_conn_id='lambda_ThAnomalies', client_type='lambda')
    credentials = aws_hook.get_credentials()
    logging.info(credentials)
    lambda_client = boto3.client('lambda',
                            region_name='us-east-1',
                            aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key,
                                aws_session_token=credentials.token)
    response_1 = lambda_client.invoke(FunctionName='lambda_realTime_merge',InvocationType='RequestResponse')
    print ('Response--->' , response_1)


"""
Python operators- initiate functions
"""

greet_task = PythonOperator(
    task_id="start_task",
    python_callable=start,
    dag=dag_realTime)

thermalAnomalies = PythonOperator(
    task_id='ThAnomalies_trigger',
    python_callable=lambda_ThAnomalies_invoke,
    provide_context=True,
    dag=dag_realTime
)

WeatherInformation = PythonOperator(
    task_id='Weather_Information_trigger',
    python_callable=lambda_weather_invoke,
    provide_context=True,
    dag=dag_realTime
)

MergeTables = PythonOperator(
    task_id='Merge_tables_trigger',
    python_callable=lambda_merge_invoke,
    provide_context=True,
    dag=dag_realTime
)


"""
DAG order
"""
greet_task >> thermalAnomalies >> WeatherInformation >> MergeTables

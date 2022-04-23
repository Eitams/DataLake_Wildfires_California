import datetime
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import boto3



'''''
Historical data pipeline DAG
The DAG invoke 3 lambda function which extract, transform and load data from 3 api's

Pseudo code
- Start DAG on a monthly basis
- Initiate lambda functions: Lambda_etl_current_data_drought and Lambda_etl_current_data_Fire
- Initiate lambda function: lambda_updated_weather
'''''

## Create DAG
dag_historical = DAG(
    dag_id='Historical_wildfires_data',
    start_date = (datetime.datetime.now() - datetime.timedelta(days=1)),
    schedule_interval="@monthly"
)

'''''
Functions
'''''
def start():
    logging.info('Starting the DAG')


## functions to invoke the aws lambdas
def lambda_drought_invoke():
    ## Extract aws credentials from airflow admin connections
    aws_hook = AwsBaseHook(aws_conn_id='lambda_Historical', client_type='lambda')
    credentials = aws_hook.get_credentials()
    logging.info(credentials)
    ## Create a connection to aws using the credentials
    lambda_client = boto3.client('lambda',
                            region_name='us-east-1',
                            aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key,
                            aws_session_token=credentials.token)
    ## Invoke lambda function
    response_1 = lambda_client.invoke(FunctionName='Lambda_etl_current_data_drought',InvocationType='RequestResponse')
    print ('Response--->' , response_1)

def lambda_wildfires_invoke():
    ## Extract aws credentials from airflow admin connections
    aws_hook = AwsBaseHook(aws_conn_id='lambda_Historical', client_type='lambda')
    credentials = aws_hook.get_credentials()
    logging.info(credentials)
    ## Create a connection to aws using the credentials
    lambda_client = boto3.client('lambda',
                            region_name='us-east-1',
                            aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key,
                            aws_session_token=credentials.token)
    ## Invoke lambda function
    response_1 = lambda_client.invoke(FunctionName='Lambda_etl_current_data_Fire',InvocationType='RequestResponse')
    print ('Response--->' , response_1)

def lambda_weather_invoke():
    aws_hook = AwsBaseHook(aws_conn_id='lambda_weather', client_type='lambda')
    credentials = aws_hook.get_credentials()
    logging.info(credentials)
    # print(credentials.access_key)
    lambda_client = boto3.client('lambda',
                            region_name='us-east-1',
                            aws_access_key_id=credentials.access_key,
                            aws_secret_access_key=credentials.secret_key,
                                aws_session_token=credentials.token)
    response_1 = lambda_client.invoke(FunctionName='lambda_updated_weather',InvocationType='RequestResponse')
    print ('Response--->' , response_1)

"""
Python operators- initiate functions
"""
greet_task = PythonOperator(
    task_id="start_task",
    python_callable=start,
    dag=dag_historical)

drought = PythonOperator(
    task_id='Invoke_lambda_drought',
    python_callable=lambda_drought_invoke,
    provide_context=True,
    dag=dag_historical
)

wildfires = PythonOperator(
    task_id='Invoke_lambda_wildfires',
    python_callable=lambda_drought_invoke,
    provide_context=True,
    dag=dag_historical
)

weather = PythonOperator(
    task_id='Invoke_lambda_weather',
    python_callable=lambda_drought_invoke,
    provide_context=True,
    dag=dag_historical
)


"""
DAG order
"""
greet_task >> drought
greet_task >> wildfires
wildfires >> weather

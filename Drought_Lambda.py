import json
import psycopg2
import pandas as pd
import numpy as np
import requests
import rds_config
import datetime
from datetime import date
from datetime import datetime, timedelta
import logging


# import logger


def lambda_handler(event, context):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Definition of the time frame of last month: Starte and End Date

    EndDate = date.today()  # DAG all 30 days
    useEndDate = EndDate.strftime('X%m/X%d/%Y').replace('X0', 'X').replace('X', '')

    StartDate = date.today() - timedelta(days=30)
    useStarteDate = StartDate.strftime('X%m/X%d/%Y').replace('X0', 'X').replace('X', '')

    # Since the URL can be directly adjusted based on the date which is wanted - not the complete data set have to be loaded, only the last week can be sourced.

    # URL parts
    a = "https://usdmdataservices.unl.edu/api/CountyStatistics/GetDroughtSeverityStatisticsByAreaPercent?aoi=CA&startdate="
    b = useStarteDate
    c = "&enddate="
    d = useEndDate
    e = "&statisticsType=1"

    url = a + b + c + d + e

    ## import data set
    try:
        response = requests.get(url)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
    except requests.Error as err:
        logger.error(err)
    else:
        print('Success of loading the dataset.')

    ## Transform data to dataframe

    data_currentweek = response.json()

    df_drought_currentweek = pd.json_normalize(data_currentweek)
    df_drought_currentweek

    # add a colum with the data of upload

    df_drought_currentweek['dateOfUpload'] = date.today()  # Y-M-D
    df_drought_currentweek

    # Upload to RDS

    #    try:
    #        conn = psycopg2.connect(host="firedb.cscdg7rb1snc.us-east-1.rds.amazonaws.com", user=rds_config.name, password=rds_config.password, dbname=rds_config.db_name, connect_timeout=5)
    #       # conn = psycopg2.connect("host=firedb1.cscdg7rb1snc.us-east-1.rds.amazonaws.com  dbname=FIREDB1 user=maren password=0123456789")
    #    except psycopg2.Error as e:
    #        print("Error: Could not make connection to the Postgres database")
    #        print(e)

    # Connect to Postgresql

    try:
        conn = psycopg2.connect(host=rds_config.rds_host, user=rds_config.name, password=rds_config.password,
                                dbname=rds_config.db_name, connect_timeout=5)
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    # Auto commit is very important
    conn.set_session(autocommit=True)

    # creat the firt table (total)
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Drought_History ( MapDate varchar(10) ,FIPS varchar(10), County varchar(255) ,State varchar(10), None FLOAT ,D0 FLOAT ,D1 FLOAT ,D2 FLOAT ,D3 FLOAT ,D4 FLOAT,ValidStart DATE,ValidEnd DATE ,StatisticFormatID INTEGER, dateOfUpload DATE)")

    # upload the data into the table in RDS

    data = df_drought_currentweek

    # def. columns
    cols = ",".join([str(i) for i in data.columns.tolist()])

    #  df will be loaded to RDS
    for i, row in data.iterrows():
        sql = "INSERT INTO Drought_History  (" + cols + ") VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

        # changes should be stored
        conn.commit()

    # Close the connection
    cur.close()
    conn.close()

    return {
        'statusCode': 200,
        # 'body': json.dumps('{}'.format(ENDPOINT))
        'body': json.dumps('it works fine')
    }


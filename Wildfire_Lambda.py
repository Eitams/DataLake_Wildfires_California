import json
import pandas as pd
import numpy as np
import requests
import psycopg2
import geodata
import logging
from datetime import date, timedelta
import rds_config


def lambda_handler(event, context):
    # TODO implement

    response = requests.get('https://opendata.arcgis.com/datasets/d8fdb82b3931403db3ef47744c825fbf_0.geojson')
    data = response.json()

    df = pd.json_normalize(data['features'])

    # Data reducing
    # standardize colum names and reduce dataset
    # 1) reduce the columns -> only the relevant once will be used

    df_2 = df[["properties.OBJECTID",
               "properties.ContainmentDateTime",
               "properties.ControlDateTime",
               "properties.DailyAcres",
               "properties.DiscoveryAcres",
               "properties.FireCause",
               "properties.FireCauseGeneral",
               "properties.FireCauseSpecific",
               "properties.FireDiscoveryDateTime",
               "properties.FireMgmtComplexity",
               "properties.FireOutDateTime",
               "properties.IncidentName",
               "properties.IncidentTypeCategory",
               "properties.IncidentTypeKind",
               "properties.InitialLatitude",
               "properties.InitialLongitude",
               "properties.InitialResponseAcres",
               "properties.POOCity",
               "properties.POOCounty",
               "properties.POOLandownerKind",
               "properties.POOState"]]

    # rename the column names - no real changes, only simplified names
    df_2.rename(columns={'properties.OBJECTID': 'OBJECTID ',
                         'properties.ContainmentDateTime': 'ContainmentDateTime',
                         'properties.ControlDateTime': 'ControlDateTime',
                         'properties.DailyAcres': 'DailyAcres',
                         'properties.DiscoveryAcres': 'DiscoveryAcres',
                         'properties.FireCause': 'FireCause',
                         'properties.FireCauseGeneral': 'FireCauseGeneral',
                         'properties.FireCauseSpecific': 'FireCauseSpecific',
                         'properties.FireDiscoveryDateTime': 'FireDiscoveryDateTime',
                         'properties.FireMgmtComplexity': 'FireMgmtComplexity',
                         'properties.FireOutDateTime': 'FireOutDateTime',
                         'properties.IncidentName': 'IncidentName',
                         'properties.IncidentTypeCategory': 'IncidentTypeCategory',
                         'properties.IncidentTypeKind': 'IncidentTypeKind',
                         'properties.InitialLatitude': 'InitialLatitude',
                         'properties.InitialLongitude': 'InitialLongitude',
                         'properties.InitialResponseAcres': 'InitialResponseAcres',
                         'properties.POOCity': 'POOCity',
                         'properties.POOCounty': 'POOCounty',
                         'properties.POOLandownerKind': 'POOLandownerKind',
                         'properties.POOState': 'POOState'}, inplace=True)

    # 2) Since the focus of this project is on Wildfire in California which are triggered by natural causes, the following selection is choosen.

    df_3 = df_2.copy()
    df_4 = df_3[
        (df_3['POOState'] == 'US-CA') & (df_3['FireCause'] == 'Natural') & (df_3['IncidentTypeCategory'] == 'WF')]
    # df_4.reset_index(inplace=True)
    df_4

    # 3) Exclusion of lightning strike -> Natural wildfire also contain lightning strike, which are now expluded.

    df_4['FireCauseGeneral'].unique()
    filter_FireCause = [None, 'Other Natural Cause']

    df_5 = df_4[df_4.FireCauseGeneral.isin(filter_FireCause)]

    # Data cleaning/transformation:
    # 1)  Initial Latitude & Longitude ->  there are entries with longetide and lotitude combinations which not lie in California, hence this entries are excluded.

    df_5[["InitialLatitude", "InitialLongitude"]].describe()
    test_georaphi = df_5[["InitialLatitude", "InitialLongitude"]]
    df_6 = df_5[(df_5['InitialLatitude'] > 20) & (df_5['InitialLongitude'] < -100)]
    df_6

    # 2) transformtion of the dates, since in the weather data will be sourced on daily basis, the dates get tranformed to the commen standard.
    df_7 = df_6.copy()
    # ContainmentDateTime
    df_7["ContainmentDateTime"] = pd.to_datetime(df_7["ContainmentDateTime"])
    df_7["ContainmentDateTime"] = df_7["ContainmentDateTime"].dt.date
    # ControlDateTime
    df_7["ControlDateTime"] = pd.to_datetime(df_7["ControlDateTime"])
    df_7["ControlDateTime"] = df_7["ControlDateTime"].dt.date
    # FireDiscoveryDateTime
    df_7["FireDiscoveryDateTime"] = pd.to_datetime(df_7["FireDiscoveryDateTime"])
    df_7["FireDiscoveryDateTime"] = df_7["FireDiscoveryDateTime"].dt.date
    # FireOutDateTime
    df_7["FireOutDateTime"] = pd.to_datetime(df_7["FireOutDateTime"])
    df_7["FireOutDateTime"] = df_7["FireOutDateTime"].dt.date

    # 3) only rows with complete date entries will be considered

    df_8 = df_7.dropna(subset=['ContainmentDateTime', 'ControlDateTime', 'FireDiscoveryDateTime', 'FireOutDateTime'])
    df_8

    # 4) reset index

    df_8 = df_8.reset_index(drop=True)

    # 5) add a colum with the data of upload
    from datetime import date
    df_8['dateOfUpload'] = date.today()  # Y-M-D

    # Lambda: last 30 days - take only the entries from the last 30 days (DAG is running every 30 days)
    LastUpDateDate = date.today() - timedelta(days=31)

    df_lambda = df_8[(df_8['FireDiscoveryDateTime'] > LastUpDateDate)]

    try:
        conn = psycopg2.connect(host=rds_config.rds_host, user=rds_config.name, password=rds_config.password,
                                dbname=rds_config.db_name, connect_timeout=5)
    except psycopg2.Error as e:
        print("Error: Could not make connection to the Postgres database")
        print(e)

    assert (conn)

    try:
        cur = conn.cursor()
    except psycopg2.Error as e:
        print("Error: Could not get curser to the Database")
        print(e)

    # Auto commit is very important
    conn.set_session(autocommit=True)
    # Creat table if not exists
    cur.execute(
        "CREATE TABLE IF NOT EXISTS Firedata_History ( OBJECTID INTEGER, ContainmentDateTime DATE, ControlDateTime DATE, DailyAcres FLOAT, DiscoveryAcres Float, FireCause varchar(30), FireCauseGeneral VARCHAR (255), FireCauseSpecific VARCHAR (255), FireDiscoveryDateTime DATE, FireMgmtComplexity VARCHAR (255), FireOutDateTime DATE, IncidentName VARCHAR (255), IncidentTypeCategory VARCHAR (2), IncidentTypeKind VARCHAR (5), InitialLatitude Float, InitialLongitude Float, InitialResponseAcres Float, POOCity VARCHAR (255), POOCounty VARCHAR (255), POOLandownerKind VARCHAR (255), POOState VARCHAR (10), dateOfUpload DATE)")
    # Upload the data to RDS

    data = df_lambda

    # columns are def.
    cols = ",".join([str(i) for i in data.columns.tolist()])

    #  data will be stored in RDS
    for i, row in data.iterrows():
        sql = "INSERT INTO Firedata_History  (" + cols + ") VALUES (" + "%s," * (len(row) - 1) + "%s)"
        cur.execute(sql, tuple(row))

        # save the changes.
        conn.commit()

    cur.close()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

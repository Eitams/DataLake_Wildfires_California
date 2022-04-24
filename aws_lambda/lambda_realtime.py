import json
import urllib.request
import pandas as pd
import boto3
import psycopg2
import config
import io
import os
from functions import API_query_forecast
from functions import json_header_extractor
from functions import json_data_extractor

# 0. Acess environment variable
s3_name=os.environ['s3_bucket_name']
access_key=os.environ['aws_access_key_id']
secret_key=os.environ['aws_secret_access_key']
token=os.environ['aws_session_token']
ENDPOINT = os.environ['ENDPOINT']
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']
APIKEY = os.environ['APIK'] 


def lambda_handler(event, context):
    
    # 1. Prompt data from S3 bucket-->ThermoAnomalies
    s3_client =boto3.client('s3')
    s3_bucket_name=s3_name
    s3 = boto3.resource('s3',
                    aws_access_key_id=access_key,
                    aws_secret_access_key=secret_key,
                    aws_session_token=token)
    
    my_bucket=s3.Bucket(s3_bucket_name)
    
    #file of interest 'ThAnomalies3.csv'
    obj = s3.Object(s3_bucket_name,key='ThAnomalies3.csv')
    data=obj.get()['Body'].read()
    df=pd.read_csv(io.BytesIO(data), header=0, delimiter=",", low_memory=False)
    thdf= df.drop('Unnamed: 0', axis=1)
    
    print('Data from S3 successfully accessed!')
    
    #2. Load ThermoAnomalies data in RDS 
    try: 
        print("dbname={} user={}".format(DB_NAME, USERNAME))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e) 
    try: 
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get curser to the Database")
        print(e)
    
    conn.set_session(autocommit=True)
    
    cur.execute("CREATE TABLE IF NOT EXISTS THERMOANOMALIES_DATA (OBJECTID INT, BRIGHTNESS FLOAT, SCAN FLOAT, TRACK FLOAT, SATELLITE VARCHAR(10), CONFIDENCE FLOAT, VERSION VARCHAR(15), BRIGHT_T31 FLOAT, FRP FLOAT, ACQ_DATE FLOAT, DAYNIGHT VARCHAR(10), x FLOAT, y FLOAT)")  
    #Upload data in the table
    data=thdf

    cols = ",".join([str(i) for i in data.columns.tolist()])
    imported_rows=0
    for i,row in data.iterrows():
        try:
            sql = "INSERT INTO THERMOANOMALIES_DATA  (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cur.execute(sql, tuple(row))
            conn.commit()
            imported_rows+=1
        except:
         pass
     
    cur.execute("select count(*) from THERMOANOMALIES_DATA;")
    number_rows=cur.fetchall()[0][0]
    print('Number of rows in the table THERMOANOMALIES_DATA',number_rows)
    print('Imported rows: ',imported_rows)
    
    print('ThAnomalies data successfully loaded to RDS!')
    #Access latitude and longitute
    long=df.loc[:,'x']
    lat=df.loc[:,'y']
    locations_lst=[]
    for i,j in enumerate(lat):
        lat_long=str(j)+','+str(long[i])
        locations_lst.append(lat_long)
    
    #3. Prompt data from the weather API
    forecastDays=3
    n=0
    raw_data=[]
    for loc in locations_lst:           #location_lst --> locations from S3 buckets
        URL=API_query_forecast(loc, str(forecastDays))
        print(URL)
        response = urllib.request.urlopen(URL)
        data = response.read()
        weatherData = json.loads(data.decode('utf-8'))
        if n==0: 
            header=json_header_extractor()
            data=json_data_extractor()
            raw_data.extend(data)
            n+=1
        else:
            data=json_data_extractor()
            raw_data.extend(data)
    
    #Build DateFrame and procceed to minor manipulations to load the data in RDS
    df=pd.DataFrame(raw_data,columns=header)
    df_manip=df
    
    #Quick manipulation in order to facilitate the insertion  in the RDS tables (delete columns with no data, and re-arrange column order)
    # delete 'stationContribution', 'info', 'index', 'distance', 'time', 'currentConditions' , 'alerts' because they are empty columns -> no data
    df_manip=df_manip.drop(['stationContributions', 'datetime', 'index', 'distance', 'time','stations', 'alerts'], axis=1)
    
    # re-arrange column
    re_arrg_col=['tz','longitude','latitude','name','address','id']
    for col in re_arrg_col:
        shiftpos=df_manip.pop(col)
        df_manip.insert(0,col,shiftpos)
    
    # rename some columns (ex: 2 times wdir --> wdir & wdir_current)
    # the last part of the table are current conditons (starts after 'cape' column)
    index_current_cond=df_manip.columns.get_loc('cape')
    lst_to_rename=list(df_manip.columns[index_current_cond:])
    for i,col in enumerate(lst_to_rename):
        if col in ['cape','sunrise','moonphase','sunset']:
            pass
        else:
            new_col=col+'_current'
            lst_to_rename[i]=new_col
    lst_not_modify=list(df_manip.columns[:index_current_cond])
    new_columns=lst_not_modify+lst_to_rename
    df_manip.columns=new_columns
    
    new_data=df_manip
    
    print('Data from API successfully accessed and DataFrame built!')
    
    #4. Load weather data in RDS
    try: 
        print("dbname={} user={}".format(DB_NAME, USERNAME))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e)
    try: 
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get curser to the Database")
        print(e)

    conn.set_session(autocommit=True)
    
    cur.execute("CREATE TABLE IF NOT EXISTS REALTIME_WEATHER_DATA (id VARCHAR(70), address VARCHAR(80), name VARCHAR(70), latitude FLOAT, longitude FLOAT, tz VARCHAR(30), wdir FLOAT,uvindex FLOAT, datetimeStr DATE, preciptype VARCHAR(50), cin FLOAT, cloudcover FLOAT, pop FLOAT, mint FLOAT, precip FLOAT, solarradiation FLOAT, dew FLOAT, humidity FLOAT, temp FLOAT, maxt FLOAT, visibility FLOAT, wspd FLOAT,severerisk INT, solarenergy FLOAT, heatindex FLOAT, snowdepth FLOAT, sealevelpressure FLOAT, snow FLOAT, wgust FLOAT, conditions VARCHAR(90), windchill FLOAT, cape FLOAT, wdir_current FLOAT, temp_current FLOAT, sunrise TIMESTAMP, visibility_current INT, wspd_current FLOAT, icon_current VARCHAR(20), heatindex_current FLOAT, cloudcover_current FLOAT, precip_current FLOAT, moonphase FLOAT, snowdepth_current FLOAT, sealevelpressure_current FLOAT, dew_current FLOAT, sunset TIMESTAMP, humidity_current FLOAT, wgust_current FLOAT, windchill_current FLOAT);")
    #cur.execute('TRUNCATE TABLE REALTIME_WEATHER_DATA')
    #Upload data in the table
    data=new_data
    
    cols = ",".join([str(i) for i in data.columns.tolist()])
    imported_rows=0
    for i,row in data.iterrows():
        try:
            sql = "INSERT INTO REALTIME_WEATHER_DATA  (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cur.execute(sql, tuple(row))
            conn.commit()
            imported_rows+=1
        except:
            pass
    #Run a little check
    cur.execute("select count(*) from REALTIME_WEATHER_DATA;")
    number_rows=cur.fetchall()[0][0]
    print('Number of rows in the table REALTIME_WEATHER_DATA',number_rows)
    print('Imported rows: ',imported_rows)
    
    # END
    cur.close()
    conn.close()

    return 'ThermoAnomalies accessed and loaded & Weather Data accessed and loaded in '
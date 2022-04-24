import os
import sys
import json
import psycopg2
import urllib.request
import pandas as pd
from datetime import date, datetime, timedelta
from functions import API_query_history
from functions import json_data_extractor
from functions import json_header_extractor

## Access environmental variables
ENDPOINT_FETCH = os.environ['ENDPOINT_FETCH']
DB_NAME_FETCH = os.environ['DB_NAME_FETCH']
USERNAME_FETCH = os.environ['USERNAME_FETCH']
PASSWORD_FETCH = os.environ['PASSWORD_FETCH']

#Global variables
today=date.today()
month_ago=today-timedelta(days=30)

## lamda function
def lambda_handler(event, context):
    
    #Connect
    try: 
        print("dbname={}".format(DB_NAME_FETCH))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT_FETCH, DB_NAME_FETCH, USERNAME_FETCH, PASSWORD_FETCH))
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
    
    #Fetch data from WildFire Database
    try: 
        cur.execute("SELECT FireDiscoveryDateTime, InitialLatitude, InitialLongitude FROM Firedata_History;")
    except psycopg2.Error as e: 
        print("Error: ")
        print (e)

    row = cur.fetchone()
    data=[]
    while row:
        data.append(list(row))
        row = cur.fetchone()
    header=['date','lat','long']
    df=pd.DataFrame(data, columns=header)
    pd.to_datetime(df['date'])
    cur.close()
    conn.close()
    
    ## Data manipulation
    request_data=df[df.date.between(month_ago,today)]
    
    ## Access the API and extract the data from the .json, according to Wildefire location (ONLY for the last month)
    n=0
    delta=2
    raw_data=[]
    if len(request_data)>0:
        no_data=False  
        for fire in request_data:
            date=fire[0]
            lat=fire[1]
            long=fire[2]
            loc=str(lat)+','+str(long)
            URL=API_query_history(loc, date, delta)
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
    else:
        print('No new data this month, DATALAKE up to date!')
        return 'lamda done!'      #No need to continue if there is no data to insert
        
    df=pd.DataFrame(raw_data,columns=header)
    df_manip=df
    del df
    #Quick manipulation in order to facilitate the insertion  in the RDS tables (delete columns with no data, and re-arrange column order)
    # delete 'stationContribution', 'info', 'index', 'distance', 'time', 'currentConditions' , 'alerts' because they are empty columns -> no data
    df_manip=df_manip.drop(['stationContributions', 'datetime','info', 'index', 'distance', 'time', 'currentConditions' , 'alerts'], axis=1)
    
    # re-arrange column
    re_arrg_col=['tz','longitude','latitude','name','address','id']
    for col in re_arrg_col:
        shiftpos=df_manip.pop(col)
        df_manip.insert(0,col,shiftpos)
    new_df=df_manip
    del df_manip
    ## 3. Load new data in RDS table
    try: 
        print("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USERNAME, PASSWORD))
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
        
    #Upload data in the table
    data=new_df
    del new_df
    
    cols = ",".join([str(i) for i in data.columns.tolist()])
    
    imported_rows=0
    excluded_rows_index=[]
    for i,row in data.iterrows():
        try:
            sql = "INSERT INTO HISTORICAL_WEATHER_DATA  (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
            cur.execute(sql, tuple(row))
            conn.commit()
            imported_rows+=1
        except:
            excluded_rows_index.append(i)
            print('row %i not imported'%i)
    print('Number of imported rows: ',imported_rows)
    
    cur.close()
    conn.close()
    
    return 'lambda done!'
"""
This python script extract data from RDS table to access the corresponding
locations and dates in time to prompt weather data from Visual Crossing API,
and then store it also in a RDS table.
"""

from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import urllib.request
import json

from requests import delete
from utils import *

### Environment variable
load_dotenv('/.env')
ENDPOINT_FETCH = os.getenv('ENDPOINT_FETCH')
DB_NAME_FETCH = os.getenv('DB_NAME_FETCH')
USERNAME_FETCH = os.getenv('USERNAME_FETCH')
PASSWORD_FETCH = os.getenv('PASSWORD_FETCH')
ENDPOINT = os.getenv('ENDPOINT')
DB_NAME = os.getenv('DB_NAME')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
print(USER)
### 1. Get data from RDS table, 2. Get data from API, 3. Load in RDS table
def main(delete=False):

    ## 1. Connect to wildfire database
    try: 
        print("dbname={}".format(DB_NAME_FETCH))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(
            ENDPOINT_FETCH, DB_NAME_FETCH, USERNAME_FETCH, PASSWORD_FETCH))
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e)

    try: 
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get curser to the Database")
        print(e)

    conn.set_session(autocommit=True)

    try: 
        cur.execute("SELECT FireDiscoveryDateTime, InitialLatitude, InitialLongitude FROM Firedata_History;")
    except psycopg2.Error as e: 
        print("Error: select *")
        print (e)

    row = cur.fetchone()
    data=[]
    while row:
        data.append(list(row))
        row = cur.fetchone()

    cur.close()
    conn.close()

    ## 2. Get data from API
    n=0
    delta=2
    raw_data=[]
    error=0
    for fire in data:       #Based on the data fetched above
        date=str(fire[0])
        lat=fire[1]
        long=fire[2]
        loc=str(lat)+','+str(long)
        if error>10 or n>=200:         #If there is more than 10 errors, there is something is off and n>=300 to limit the number of calls
            break
        try:
            URL=API_query_history(loc, date, delta)
            print("Fetching...")
            response = urllib.request.urlopen(URL)
            data = response.read()
            weatherData = json.loads(data.decode('utf-8'))
            if n==0:
                header=json_header_extractor(weatherData)
                data=json_data_extractor(weatherData, delta)
                raw_data.extend(data)
                n+=1                    #to use the header function only once
            else:
                data=json_data_extractor(weatherData, delta)
                raw_data.extend(data)
                n+=1                    #to count how many request were made, so we don't go over daily cost
        except:
            print('Something went wrong!')
            print('Can be the location, the date or the request with the API')
            error+=1
    # Some quick manipulations
    df=pd.DataFrame(raw_data,columns=header)
    df_manip=df

    df_manip=df_manip.drop(['stationContributions', 'datetime','info', 'index', 'distance', 'time', 'currentConditions' , 'alerts'], axis=1)

    # re-arrange column
    re_arrg_col=['tz','longitude','latitude','name','address','id']
    for col in re_arrg_col:
        shiftpos=df_manip.pop(col)
        df_manip.insert(0,col,shiftpos)
    new_data=df_manip

    ## 3. Load the new data in the RDS table HISTORICAL_WEATHER_DATA
    try: 
        print("dbname={}".format(DB_NAME))
        conn = psycopg2.connect("host={} dbname={} user={} password={}".format(ENDPOINT, DB_NAME, USER, PASSWORD))
    except psycopg2.Error as e: 
        print("Error: Could not make connection to the Postgres database")
        print(e)
    try:
        cur = conn.cursor()
    except psycopg2.Error as e: 
        print("Error: Could not get curser to the Database")
        print(e)
    
    conn.set_session(autocommit=True)
    cur.execute("CREATE TABLE IF NOT EXISTS HISTORICAL_WEATHER_DATA (id VARCHAR(70), address VARCHAR(80), name VARCHAR(70), latitude FLOAT, longitude FLOAT, tz VARCHAR(30), wdir FLOAT, temp FLOAT, maxt FLOAT, visibility FLOAT, wspd FLOAT, datetimeStr DATE, solarenergy FLOAT, heatindex FLOAT, cloudcover FLOAT, mint FLOAT, precip FLOAT, solarradiation FLOAT, weathertype VARCHAR(120),	snowdepth FLOAT, sealevelpressure FLOAT, snow FLOAT, dew FLOAT, humidity FLOAT, precipcover FLOAT, wgust FLOAT, conditions VARCHAR(90), windchill FLOAT);")
    if delete==True:
        print('data in HISTORICAL_WEATHER_DATA deleted!')
        cur.execute('TRUNCATE TABLE HISTORICAL_WEATHER_DATA')
    else:
        pass
    
    data=new_data
    del new_data

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

    del data

    return "END"



if __name__=="__main__":
    print('If you run this script, it will fetch more than thousand rows and insert it in a RDS table.')
    print('/!\ it might insert a lot of duplicates!')
    user_input=input('Would you like to empty the table first? [yes/no]: ')
    if user_input=='yes':
        print('You are about to delete the data in the table <HISTORICAL_WEATHER_DATA>')
        confirm=input('To confirm type <delete>: ')
        if confirm=='delete':
            main(delete=True)
        else:
            print('Canceled!')
    else:
        print('You are about to insert more than thousands rows in a table with existing data!')
        user_input=input('To continue type <yes> or hit <ENTER> to leave: ')
        if user_input=='yes':
            main(delete=False)
        else:
            exit()

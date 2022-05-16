import os
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

'''
The lambda function is executed in an AWS colud environment, Environmantel varibales are defined when creating the lambda function
This function extract the thermal anomaly table and the rel time corresponding weather table from Nathan RDS, then clean the tables,
merge them, perform feature engineering and load them back into an RDS.
'''

def lambda_handler(event, context):
    #---------------------------    
    ## Access environmental variables
    #---------------------------
    ENDPOINT = os.environ['ENDPOINT']
    DB_NAME = os.environ['DB_NAME']
    USER = os.environ['USER']
    PASSWORD = os.environ['PASSWORD']
    
    ENDPOINT_EITAM = os.environ['ENDPOINT_EITAM']
    DB_EITAM = os.environ['DB_EITAM']
    USER_EITAM = os.environ['USER_EITAM']
    PASSWORD_EITAM = os.environ['PASSWORD_EITAM']
    
    #---------------------------
    # Extract tables (RealTime THanomalies and weather)
    #---------------------------
    ## create engine - connect to realtime RDS - Nathan db
    engine = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER, PASSWORD, ENDPOINT, DB_NAME ))
    con = engine.connect()
    
    ## Extract tables into dataframes
    df_weather = pd.read_sql("SELECT * FROM REALTIME_WEATHER_DATA ;", con)
    df_THanomalies = pd.read_sql("SELECT * FROM THERMOANOMALIES_DATA ;", con)
    
    con.close()

    #---------------------------   
    ## Transform data
    #---------------------------
    ## extracting only wanted columns from wildfires df
    df1 = df_THanomalies[
        [
            "objectid", "x", "y", "brightness"
        ]
    ].copy()
    
    ## extracting only wanted columns from wheather df
    df2 = df_weather[
        [
            "latitude",
            "longitude",
            "datetimestr",
            "temp",
            "maxt",
            "precip",
            "dew",
            "humidity",
            "wgust",
            "wdir",
            "wspd",
            "sealevelpressure"
    
        ]
    ].copy()
    
    ## Matching column names for both tables
    df1.columns = ["objectid", "longitude", "latitude", "brightness"]
    
    ## Create new features: forecast values - one and two days ahead
    df2["tmp_plus_1"] = df2["temp"].shift(-1)
    df2["tmp_plus_2"] = df2["temp"].shift(-2)
    
    df2["maxt_plus_1"] = df2["maxt"].shift(-1)
    df2["maxt_plus_2"] = df2["maxt"].shift(-2)
    
    df2["precip_plus_1"] = df2["precip"].shift(-1)
    df2["precip_plus_2"] = df2["precip"].shift(-2)
    
    df2["dew_plus_1"] = df2["dew"].shift(-1)
    df2["dew_plus_2"] = df2["dew"].shift(-2)
    
    df2["humidity_plus_1"] = df2["humidity"].shift(-1)
    df2["humidity_plus_2"] = df2["humidity"].shift(-2)
    
    df2["wgust_plus_1"] = df2["wgust"].shift(-1)
    df2["wgust_plus_2"] = df2["wgust"].shift(-2)
    
    df2["wdir_plus_1"] = df2["wdir"].shift(-1)
    df2["wdir_plus_2"] = df2["wdir"].shift(-2)
    
    df2["wspd_plus_1"] = df2["wspd"].shift(-1)
    df2["wspd_plus_2"] = df2["wspd"].shift(-2)
    
    ## Drop duplicate rows (forecest rows that we turned into columns)
    df2 = df2.drop_duplicates(["latitude", "longitude"]).reset_index().drop("index", axis=1)
    
    ## Merge both dataframes and remove duplicate columns
    df_merge = pd.concat([df1, df2], axis=1 )
    df_merge = df_merge.loc[:,~df_merge.columns.duplicated()]
    
    #---------------------------
    ## Load new table to RDS 
    #---------------------------
    ## Connect to Eitam's RDS using SQLalchemy
    engine = create_engine("postgresql+psycopg2://{}:{}@{}/{}".format(USER_EITAM, PASSWORD_EITAM, ENDPOINT_EITAM, DB_EITAM ))
    con = engine.connect()
    
    ## Upload table into database
    df_merge.to_sql(name="RealTimeMerged",con=con,if_exists='replace')
    con.close() 
    
    
    return {
        'statusCode': 200
    }

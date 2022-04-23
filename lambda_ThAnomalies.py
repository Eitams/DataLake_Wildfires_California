import json
import requests
import pandas as pd
import boto3
from io import StringIO
import s3fs

'''
Lambda function to extract thermal anomalies data around California from ArcGIS api -NIFC website-
and save in in an S3 bucket
'''

def lambda_handler(event, context):
    ## TODO implement
    ## Thermal anomalies end point
    ## Setting the query results to return data points in California (poligon box)
    ## geometry=-123.70460225718664%2C%2031.91240087268507%2C-114.10255196422095%2C%2042.0471346343454&geometryType=esriGeometryEnvelope
    EndPoint = 'https://services9.arcgis.com/RHVPKKiFTONKtxq3/arcgis/rest/services/MODIS_Thermal_v1/FeatureServer/0/query?where=1%3D1&outFields=*&geometry=-123.70460225718664%2C%2031.91240087268507%2C-114.10255196422095%2C%2042.0471346343454&geometryType=esriGeometryEnvelope&inSR=4326&spatialRel=esriSpatialRelIntersects&outSR=4326&f=json'
    response = requests.get(EndPoint)  ## request data from api
    values = response.json()  ## Store json response in a variable

    ########################
    ## Preprocessing steps to convert geojson file to df
    ########################
    ## Grouping attributes column names and geometry column names in one list
    column1 = list(values['features'][1]['attributes'].keys())
    column2 = list(values['features'][1]['geometry'].keys())
    columns = column1 + column2

    ## Create a List of lists- Insert column names as the first list, later we append the rows of the table
    df_list = list([columns])

    ## Appending each new row as a list
    for i in values['features']:
        temp = list(i['attributes'].values()) + list(i['geometry'].values())
        df_list.append(temp)

    ## Converting List of lists to df
    df = pd.DataFrame(df_list, columns=columns)
    df = df.drop(df.index[0])  ## Dropping double column name

    ## Setting up max value of location to be extracted (300 locations by brightness)
    ## Most times the number of location will be significantly lower (5-20~)
    df_top300 = df.sort_values(by=['BRIGHTNESS'], ascending=False).head(300)

    ## Saving df as csv to s3 bucket
    ## Option 1:
    # bucket = 'eswf1' # already created on S3
    # csv_buffer = StringIO()
    # df_top300.to_csv(csv_buffer)
    # s3_resource = boto3.resource('s3')
    # s3_resource.Object(bucket, 'ThAnomalies.csv').put(Body=csv_buffer.getvalue())

    ## Option 2:
    bytes_to_write = df_top300.to_csv(None).encode()
    fs = s3fs.S3FileSystem()
    with fs.open('s3://eswf1/ThAnomalies3.csv', 'wb') as f:
        f.write(bytes_to_write)

    return {'statusCode': 200}

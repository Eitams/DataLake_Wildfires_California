"""
Cautions! This use a different URL and different settings
"""
import urllib.request
import json
import mysql.connector
from datetime import date, datetime, timedelta

BaseURL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/'

ApiKey='Z3Z4Q9PHE3MSXWKUPQNQ8434D'
# UnitGroup sets the units of the output - us or metric
UnitGroup='metric'

#Locations for the weather data. Multiple locations separated by pipe (|)
Locations='34.0536909,-118.242766|37.7790262,-122.419906'   # Los Angeles | San Francisco

#FORECAST or HISTORY
QueryType='HISTORY'

#1=hourly, 24=daily
AggregateHours='24'

#Params for history only
today = date.today()
StartDate = str(today)
n = 3
if QueryType=='HISTORY':
    EndDate = str(today+timedelta(days=-n))
else:
    EndDate = str(today+timedelta(days=n))

# Set up the specific parameters based on the type of query
if QueryType == 'FORECAST':
    print(' - Fetching forecast data')
    QueryParams = 'forecast?aggregateHours=' + AggregateHours + '&unitGroup=' + UnitGroup + '&shortColumnNames=true'
else:
    print(' - Fetching history for date: ', DateParam)

    # History requests require a date.  We use the same date for start and end since we only want to query a single date in this example
    QueryParams = 'history?aggregateHours=' + AggregateHours + '&unitGroup=' + UnitGroup +'&startDateTime=' + StartDate + 'T00%3A00%3A00&endDateTime=' + EndDate + 'T00%3A00%3A00'

Locations='&locations='+Locations

ApiKey='&key='+ApiKey

# Build the entire query
URL = BaseURL + QueryParams + Locations + ApiKey+"&contentType=json"

print(' - Running query URL: ', URL)
print()

response = urllib.request.urlopen(URL)
data = response.read()
weatherData = json.loads(data.decode('utf-8'))

### Need to be adapt for Postgre ###
print( "Connecting to mysql database")
#connect to the database. Enter your host, username and password
cnx = mysql.connector.connect(host='127.0.0.1',
    user='YOUR_USERNAME',
    passwd='YOUR_PASSWORD',
    database='weather_data_schema')

cursor = cnx.cursor()

# In this simple example, clear out the existing data in the table

delete_weather_data=("TRUNCATE TABLE `weather_data_schema`.`weather_data`")
cursor.execute(delete_weather_data)
cnx.commit()

# Create an insert statement for inserting rows of data
insert_weather_data = ("INSERT INTO `weather_data_schema`.`weather_data`"
                       "(`address`,`latitude`,`longitude`,`datetime`,`maxt`,`mint`,`temp`,`precip`,`wspd`,`wdir`,`wgust`,`pressure`)"
                       "VALUES (%(address)s, %(latitude)s, %(longitude)s, %(datetime)s, %(maxt)s,%(mint)s, %(temp)s, %(precip)s, %(wspd)s, %(wdir)s, %(wgust)s, %(pressure)s)")

# Iterate through the locations
locations = weatherData["locations"]
for locationid in locations:
    location = locations[locationid]
    # Iterate through the values (values are the time periods in the weather data)
    for value in location["values"]:
        data_wx = {
            'address': location["address"],
            'latitude': location["latitude"],
            'longitude': location["longitude"],
            'datetime': datetime.utcfromtimestamp(value["datetime"] / 1000.),
            'maxt': value["maxt"] if 'maxt' in value else 0,
            'mint': value["mint"] if 'mint' in value else 0,
            'temp': value["temp"],
            'precip': value["precip"],
            'wspd': value["wspd"],
            'wdir': value["wdir"],
            'wgust': value["wgust"],
            'pressure': value["sealevelpressure"]
        }
        cursor.execute(insert_weather_data, data_wx)
        cnx.commit()

cursor.close()
cnx.close()
print("Database connection closed")

print("Done")

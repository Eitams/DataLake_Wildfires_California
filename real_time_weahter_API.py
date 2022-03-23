import csv
import codecs
import urllib.request
import urllib.error
import sys
from datetime import date, datetime, timedelta

# Settings for Locations

# Settings for real-time data
today = date.today()
n = 3
future_days = today+timedelta(days=n)

# Settings for API
BaseURL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/'
ApiKey = 'Z3Z4Q9PHE3MSXWKUPQNQ8434D'
UnitGroup = 'metric'
Location = '34.0536909,-118.242766'
StartDate = str(today)
EndDate = str(future_days)
ContentType = "csv"
Include = "days"

# Building the query
print('')
print(' - Requesting weather : ')


def BuildingAPIquery(BaseURL, ApiKey, Location, StartDate, EndDate, UnitGroup, ContentType, Include):
    ApiQuery = BaseURL + Location
    if (len(StartDate)):
        ApiQuery += "/" + StartDate
        if (len(EndDate)):
            ApiQuery += "/" + EndDate
    ApiQuery += "?"

    if (len(UnitGroup)):
        ApiQuery += "&unitGroup=" + UnitGroup

    if (len(ContentType)):
        ApiQuery += "&contentType=" + ContentType

    if (len(Include)):
        ApiQuery += "&include=" + Include

    ApiQuery += "&key=" + ApiKey
    return ApiQuery


ApiQuery = BuildingAPIquery(BaseURL, ApiKey, Location, StartDate, EndDate, UnitGroup, ContentType, Include)

# Running the Query
print(' - Running query URL: ', ApiQuery)
print()

try:
    CSVBytes = urllib.request.urlopen(ApiQuery)
except urllib.error.HTTPError as e:
    ErrorInfo = e.read().decode()
    print('Error code: ', e.code, ErrorInfo)
    sys.exit()
except urllib.error.URLError as e:
    ErrorInfo = e.read().decode()
    print('Error code: ', e.code, ErrorInfo)
    sys.exit()


# Parse the results as CSV

CSVText = csv.reader(codecs.iterdecode(CSVBytes, 'utf-8'))

RowIndex = 0
for Row in CSVText:
    if RowIndex == 0:
        FirstRow = Row
    else:
        print('Weather in ', Row[0], ' on ', Row[1])

        ColIndex = 0
        for Col in Row:
            if ColIndex >= 4:
                print('   ', FirstRow[ColIndex], ' = ', Row[ColIndex])
            ColIndex += 1
    RowIndex += 1

# If there are no CSV rows then something fundamental went wrong
if RowIndex == 0:
    print('Sorry, but it appears that there was an error connecting to the weather server.')
    print('Please check your network connection and try again..')

# If there is only one CSV  row then we likely got an error from the server
if RowIndex == 1:
    print('Sorry, but it appears that there was an error retrieving the weather data.')
    print('Error: ', FirstRow)

print()
CSVBytes.to_csv(index=False, header=True)
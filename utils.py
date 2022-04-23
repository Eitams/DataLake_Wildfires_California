import os
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
load_dotenv()
APIKEY = os.getenv('APIKEY')

def API_query_history(loc, date, delta, ApiKey=APIKEY, unit='metric', agg='24'):
    # This is the core of our weather query URL
    BaseURL = 'https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/weatherdata/history'

    #UnitGroup sets the units of the output - us or metric
    UnitGroup=unit

    #Locations for the weather data. Multiple locations separated by pipe (|)
    Locations=loc

    #1=hourly, 24=daily
    AggregateHours=agg

    #Params for history only
    date_of_interest=datetime.strptime(date,'%Y-%m-%d').date()
    StartDate = str(date_of_interest+timedelta(days=-delta))
    EndDate= str(date_of_interest+timedelta(days=delta))

    # Set up the query
    QueryParams = '?aggregateHours=' + AggregateHours + '&startDateTime=' + StartDate + '&endDateTime='+ EndDate + '&unitGroup=' + UnitGroup + '&shortColumnNames=true'

    Locations='&locations=' + Locations

    ApiKey='&key='+ApiKey

    # Build the entire query
    URL = BaseURL + QueryParams + Locations + ApiKey+"&contentType=json"

    return URL

#building header, only need to loop once
def json_header_extractor(Data):
    dict_locations=Data['locations']
    key_location=list(Data['locations'].keys())
    header=[]
    for key in dict_locations[str(key_location[0])].keys():
        if key=='values':
            header.extend(list(dict_locations[str(key_location[0])][key][0].keys()))
        else:
            header.append(key)
    return header

#building data, in the 'values' section, there is the data for the forecasted days, so if forecstDays=3, there is 3 elements in the list 'values'
def json_data_extractor(Data, delta):
    dict_locations=Data['locations']
    key_location=list(Data['locations'].keys())
    all_data=[]
    for i in range((delta*2)+1):
        data=[]
        for key, value in dict_locations[str(key_location[0])].items():
            if key=='values':
                try:
                    data.extend(list(dict_locations[str(key_location[0])][key][i].values()))
                except:
                    pass
            else:
                data.append(value)
        all_data.append(data)
    return all_data
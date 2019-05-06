import urllib.request
import xmltodict, json
# import pygrib
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import time

import urllib.request
import xmltodict

# Query to extract parameter forecasts for one particular place (point)
#
# http://data.fmi.fi/fmi-apikey/f96cb70b-64d1-4bbc-9044-283f62a8c734/wfs?
#   request=getFeature&storedquery_id=fmi::forecast::hirlam::surface::point::multipointcoverage
#   &place=thessaloniki
#   &parameters="Temperature, Humidity"
#
def extract_forecasts_place(fmi_addr, my_api_key, data_format, parameters, place ):

    request = "getFeature&storedquery_id=fmi::forecast::hirlam::surface::point::" + data_format

    query_parameters = ""
    for it in range(len(parameters)-1):
        query_parameters += parameters[it] + ","

    query_parameters += parameters[len(parameters)-1]

    query = fmi_addr + my_api_key + "/wfs" + "?" + "request" + "=" + request + "&" + "place=" + place + "&" + "parameters=" + query_parameters

    print(query, "\n")

    with urllib.request.urlopen(query) as fd:
        query = xmltodict.parse(fd.read())

    return(query)

#---------------------------------------------------------------------------------------
# Query to extract parameter forecasts for a Region Of Interest (grid defined by bbox)
#
# Query made for FMI:
# http://data.fmi.fi/fmi-apikey/f96cb70b-64d1-4bbc-9044-283f62a8c734/wfs?
#   request=getFeature&storedquery_id=fmi::forecast::hirlam::surface::grid
# & crs=EPSG::4326
# & bbox='22.890701,40.574326,23.034210,40.67855'
# & parameters=Temperature,Humidity
#
def extract_forecasts_grid(fmi_addr, my_api_key, query_request, data_format, coord_sys, bbox, parameters):

    # data_format = grid
    request = query_request + data_format

    # coordinate system e.g. coord_sys = EPSG::4326
    query_crs = coord_sys

    # bbox = [22.890701,40.574326,23.034210,40.67855] --- region of Thessaloniki
    query_box = ""
    for j in range(len(bbox)-1):
        query_box += str(bbox[j]) + ","

    query_box += str(bbox[len(bbox)-1])

    query_parameters = ""
    for it in range(len(parameters) - 1):
        query_parameters += parameters[it] + ","

    query_parameters += parameters[len(parameters)-1]

    query = fmi_addr + my_api_key + "/wfs" + "?" + "request" + "=" + request + "&" + \
            "crs=" + query_crs + "&" + "bbox=" + query_box + "&" + "parameters=" + query_parameters

    print("Query made for FMI: \n{}\n".format(query))
    with urllib.request.urlopen(query) as fd:
        response = xmltodict.parse(fd.read())

    return(response)

#-----------------------------------------------------------------------------
#   Query to extract values from a grib file in data.frame (dset)
#       Columns names of data.frame are:
#           ['Measurement_Number', 'Name', 'DateTime', 'Lat', 'Long', 'Value']
#
def extract_gribs(dataDICT):

    # gml:fileReference to key for the FTP

    # path for the value we need , for downloading grib2 file
    FTPurl = dataDICT['wfs:FeatureCollection']['wfs:member'][1]['omso:GridSeriesObservation']['om:result']['gmlcov:RectifiedGridCoverage']['gml:rangeSet']['gml:File']['gml:fileReference']

    print("Query for downloading grb file with the values asked: \n{}\n".format(FTPurl))

    # Create the grib2 file
    result = urllib.request.urlopen(FTPurl)
    with open('gribtest.grib2', 'b+w') as f:
        f.write(result.read())

    gribfile = 'gribtest.grib2'  # Grib filename
    grb = pygrib.open(gribfile)

    # Creation of dictionary, for parameters : metric system
    paremeters_units = {
        "Mean sea level pressure": "Pa", "Orography": "meters", "2 metre temperature": "°C",
        "2 metre relative humidity": "%",
        "Mean wind direction": "degrees",
        "10 metre wind speed": "m s**-1",
        "10 metre U wind component": "m s**-1",
        "10 metre V wind component": "m s**-1",
        "surface precipitation amount, rain, convective": "kg m**-2", "2 metre dewpoint temperature": "°C"}

    # Create a data frame to keep all the measurements from the grib file
    dset = pd.DataFrame(columns=['Measurement_Number', 'Name', 'DateTime', 'Lat', 'Long', 'Value'])

    for g in grb:
        str_g = str(g)  # casting to str
        col1, col2, *_ = str_g.split(":")  # split the message columns

        # Temporary data.frame
        temp_ds = pd.DataFrame(columns=['Measurement_Number', 'Name', 'DateTime', 'Lat', 'Long', 'Value'])

        meas_name = col2
        meas_no = col1

        g_values = g.values
        lats, lons = g.latlons()
        g_shape = g.values.shape

        dim = g_shape[0] * g_shape[1]
        temp_ds['Measurement_Number'] = [meas_no] * dim
        temp_ds['Name'] = [meas_name] * dim
        temp_ds['DateTime'] = [ g.validDate.isoformat() + 'Z' ] * dim
        temp_ds['Lat'] = lats.flatten()
        temp_ds['Long'] = lons.flatten()
        temp_ds['Value'] = g_values.flatten()

        dset = pd.concat([dset, temp_ds], ignore_index=True)

    # close grib file
    f.close()

    return(dset)

#-----------------------------------------------------------------------------
#   Query to extract values from a place defined by lat/long coordinates
#   at specific date time period (start_datetime = current date/time) and
#   end_datetime = start_datetime + time_interval (54h)
#
#   pnts: list of dictionaries describing the lat/long coordinates
#
#   output: list of queries
#
#   http://data.fmi.fi/fmi-apikey/
#           f96cb70b-64d1-4bbc-9044-283f62a8c734
#           /wfs?request=getFeature&storedquery_id=fmi::forecast::hirlam::surface::point::timevaluepair&
#           starttime=2018-05-16T00:00:00Z & endtime=2018-05-20T23:00:00Z &
#           latlon=40.632682,22.941084 &
#           parameters=Temperature,Humidity
#
def extract_forecasts_latlng(fmi_addr, my_api_key, data_format, parameters, points, time_interval):

    request = "getFeature&storedquery_id=fmi::forecast::hirlam::surface::point::" + data_format

    # create the parameter value of the query [parameters=Temperature,Humidity]
    query_parameters = ""
    for itp in range(len(parameters) - 1):
        query_parameters += parameters[itp] + ","

    query_parameters += parameters[len(parameters) - 1]

    query_list = []
    for it in range(len(points)):

        # create the latlon value of the query parameter
        latlon_pos = str(points[it]['lat']) + "," + str(points[it]['long'])

        # create the datetime interval: start_datetime = current datetime, end_datetime = start_datetime + 54h
        now_datetime = datetime.utcnow().replace(microsecond=0)
        curr_hour = now_datetime.hour

        if curr_hour >= 0 and curr_hour < 6:
            start_datetime = now_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
            end_datetime = start_datetime + timedelta(hours=time_interval)

            start_datetime = start_datetime.isoformat() + 'Z'
            end_datetime = end_datetime.isoformat() + 'Z'

        elif curr_hour >= 6 and curr_hour < 12:
            start_datetime = now_datetime.replace(hour=6, minute=0, second=0, microsecond=0)
            end_datetime = start_datetime + timedelta(hours=time_interval)

            start_datetime = start_datetime.isoformat() + 'Z'
            end_datetime = end_datetime.isoformat() + 'Z'

        elif curr_hour >= 12 and curr_hour < 18:
            start_datetime = now_datetime.replace(hour=12, minute=0, second=0, microsecond=0)
            end_datetime = start_datetime + timedelta(hours=time_interval)

            start_datetime = start_datetime.isoformat() + 'Z'
            end_datetime = end_datetime.isoformat() + 'Z'

        else:
            start_datetime = now_datetime.replace(hour=18, minute=0, second=0, microsecond=0)
            end_datetime = start_datetime + timedelta(hours=time_interval)

            start_datetime = start_datetime.isoformat() + 'Z'
            end_datetime = end_datetime.isoformat() + 'Z'

        query = fmi_addr + my_api_key + "/wfs" + "?" + "request" + "=" + request + "&" + \
                "starttime=" + start_datetime + "&" + "endtime=" + end_datetime + "&" + \
                "latlon=" + latlon_pos + "&" + "parameters=" + query_parameters

        query_list.append(query)

        print(query, "\n")

    return(query_list)


#------------------------------------------------------------------------------------------
#   Query to extract real-time weather values from a place defined by lat/long coordinates
#   at current date time from Dark Sky API
#
# https://api.darksky.net/forecast/60bc19179a97cf8ca2e69d6f580afcf1/40.60401,22.97893?exclude=minutely,hourly,daily,alerts,flags&units=si

def extract_realtime_weather_darksky(darksky_addr, my_api_key, exclude_parameters, points):

    request = darksky_addr + my_api_key

    # create the parameter value of the query which are going to exclude
    query_excl_parameters = ""
    for itp in range(len(exclude_parameters)-1):
        query_excl_parameters += exclude_parameters[itp] + ","

    query_excl_parameters += exclude_parameters[len(exclude_parameters) - 1]

    query_list = []
    for it in range(len(points)):
        # create the latlon value of the query parameter
        latlon_pos = str(points[it]['lat']) + "," + str(points[it]['long'])

        query = request + "/" + latlon_pos + "?" + "exclude=" + query_excl_parameters + "&" + "units=si"
        print(query)

        query_list.append(query)

    return(query_list)


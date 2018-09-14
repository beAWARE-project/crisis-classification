import urllib.request
import xmltodict, json
#import pygrib
import numpy as np
import pandas as pd
from datetime import datetime
import time

# Query to extract parameter forecasts for one particular place (point)
#
# http://data.fmi.fi/fmi-apikey/f96cb70b-64d1-4bbc-9044-283f62a8c734/wfs?
#   request=getFeature&storedquery_id=fmi::forecast::hirlam::surface::point::multipointcoverage
#   &place=valencia
#   &parameters="GeopHeight, Temperature, Pressure, Humidity, WindDirection, WindSpeedMS,
#               WindUMS, WindVMS, MaximumWind, DewPoint, Precipitation1h, PrecipitationAmount"
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
# & bbox=-0.439453, 39.192884, -0.201874, 39.426647
# & parameters=Temperature,Humidity,WindDirection, WindSpeedMS
#
def extract_forecasts_grid(fmi_addr, my_api_key, query_request, data_format, coord_sys, bbox, parameters):

    # data_format = grid
    request = query_request + data_format

    # coordinate system e.g. coord_sys = EPSG::4326
    query_crs = coord_sys

    # bbox = [-0.439453, 39.192884, -0.201874, 39.426647] --- region of Valencia
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

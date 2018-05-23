# Version 4
# Created Date: 19/03/2018
# Modified Date: 20/03/2018
#
# Implements the 2nd algorithm of Crisis Classification module
# based on the measurements of water levels from sensors for a
# specific 3 weather stations at a) last measurement
# or b) at particular date/time period.
# If flag_phenTime = True, then the phenomenonTime of each Weather Station will be considered,
# otherwise the specific dates/time period will be examined
#
#----------------------------------------------------------------------------------------------------------
# Inputs: a) Time series of measurements of water levels by the sensors
#               for a specific weather station at last measurement (date/time)
#         b) Thresholds for the particular specific weather station and for water river level
#
# Outputs: TOP104_METRIC_REPORT for each Weather Station and Datastream, which contains:
#         a) the actual crisis level associated to the sensor position
#         b) a metric-scale from 0 to 3 depending on whether the actual value exceeds a
#            particular alarm threshold.
#
#   Algorithm 2 from Crisis Classification (based on AAWA)
#----------------------------------------------------------------------------------------------------------
#

from bus.bus_producer import BusProducer
from bus.CRCL_service import CRCLService
import json, time
from datetime import datetime, timedelta
import os, errno
from pathlib import Path

from Top104_Metric_Report import Top104_Metric_Report
from Create_Queries import extract_from_WS_Sensors, extract_stations_river, \
    extract_forecasts, extract_station_datastream, extract_station_location
from Auxiliary_functions import compare_value_scale_thresholds, Overall_Crisis_Classification_Index_WeatherStations
from collections import OrderedDict

# Create a directory to store the output files and TOPICS
root_path = Path.cwd()
#directory = "TOPICS_fromSensors_VER_4"
directory = "TOPICS_fromSensors_VER_4_2010"

os.makedirs(directory, exist_ok=True)

#-----------------------------------------------------------------------------------
# Fetch data from the OGC SensorThings API
#
# User defined values in order to formulate the query
#
service_root_URI = 'https://beaware.server.de/SensorThingsService/v1.0/'

SensorThingEntities = ['Things', 'Locations', 'HistoricalLocations',
                        'Datastreams', 'Sensor', 'Observations',
                        'ObservedProperties', 'FeaturesOfInterest', 'MultiDatastreams']

SensorThings = [SensorThingEntities[0], SensorThingEntities[3], SensorThingEntities[5]]

# Initialise arrays to store the results of comparison for each weather station and each datastream (WL or PR)
meas_ColNote_WL = []
meas_ColNote_PR = []

#--------------------------------------------------------------------------------------
# Creates the thresholds for each one of the Weather Stations of interest
#
Weather_Stations_Ids = [45, 47, 374]

Thresholds_WL = [{'ID': 45, 'Alarm1': 4.36, 'Alarm2': 4.86, 'Alarm3': 5.66},
                 {'ID': 47, 'Alarm1': 3.00, 'Alarm2': 4.60, 'Alarm3': 5.40},
                 {'ID': 374, 'Alarm1': 1.63, 'Alarm2': 3.03, 'Alarm3': 3.43}
                ]

#Thresholds_PR = [{'ID': 47, 'Alarm1': 50, 'Alarm2': 100, 'Alarm3': 150},
#                 {'ID': 49, 'Alarm1': 50, 'Alarm2': 100, 'Alarm3': 150},
#                 {'ID': 374, 'Alarm1': 50, 'Alarm2': 100, 'Alarm3': 150}
#                ]

# PEIRAGMENA THRESHOLDS
#Thresholds_WL = [{'ID': 45, 'Alarm1': 0.36, 'Alarm2': 0.6, 'Alarm3': 0.66},
#                 {'ID': 47, 'Alarm1': 0.01, 'Alarm2': 0.60, 'Alarm3': 0.80},
#                 {'ID': 374, 'Alarm1': 0.03, 'Alarm2': 0.03, 'Alarm3': 0.43}
#                ]


#---------------------------------------------------------------------------------------------------
# Step 1: Extracts the weather stations where have as Datastreams the Water Level

flag_last_measurement = True  # or False

# List of dictionaries contains the id of each WS and its one of the Datastreams.
# For WS where one of the Datastreams is missing the None value is filled
WSDS = []

# dates_WL=[]
# flag_phenTime = True

# Specify the period date/time for each Weather Station
flag_phenTime = False
#dates_WL = [{'ID': 45, 'PhenDateTime': ['2010-10-31T21:00:00.000Z', '2010-11-02T22:00:00.000Z']},
#            {'ID': 47, 'PhenDateTime': ['2010-10-31T19:00:00.000Z', '2010-11-02T23:00:00.000Z']},
#            {'ID': 374, 'PhenDateTime': ['2010-10-31T13:00:00.000Z', '2010-11-02T23:00:00.000Z']}
#            ]

dates_WL = [{'ID': 45, 'PhenDateTime': ['2010-11-01T07:00:00.000Z']},
            {'ID': 47, 'PhenDateTime': ['2010-11-01T10:00:00.000Z']},
            {'ID': 374, 'PhenDateTime': ['2010-11-02T12:00:00.000Z']}
            ]

for i, StationID in enumerate(Weather_Stations_Ids):

    WSDS_dict = {'ID': StationID}

    # extract the location of the station
    SensThings_Loc = [SensorThingEntities[0], SensorThingEntities[1]]
    selVals = {'thing_sel': ['id', 'name'], 'loc_sel': ['location']}
    filt_args = {'thing_filt': ['id']}
    filt_vals = {'thing_filt': str(StationID)}

    resp_station_loc = extract_station_location(service_root_URI, SensThings_Loc, selVals, filt_args, filt_vals)

    SensThings = [SensorThingEntities[0], SensorThingEntities[3]]
    selVals = {'dstr_sel': ['id', 'name', 'phenomenonTime']}
    filt_args={'thing_filt': ['id'], 'dstr_filt': ['name']}
    filt_vals_WL={'thing_filt': str(StationID), 'dstr_filt': ['Water']}

    resp_station_datastream_WL = extract_station_datastream(service_root_URI, SensThings, selVals, filt_args, filt_vals_WL)

    # Update WSDS with Weather Station name
    WSDS_dict.update({'WS_name': resp_station_datastream_WL['value'][0]['name']})

    # Keep elements and values for Water Level
    if len(resp_station_datastream_WL['value'][0]['Datastreams']) == 0:
         WSDS_dict.update({'WL': None})
         WSDS_dict.update({'WL_name': None})
    else:
         WSDS_dict.update({'WL': resp_station_datastream_WL['value'][0]['Datastreams'][0]['@iot.id']})
         WSDS_dict.update({'WL_name': resp_station_datastream_WL['value'][0]['Datastreams'][0]['name']})

         # Update the date/time equal with the phenomononTime
         if flag_phenTime == True:
            dates_WL_dict = {'ID': StationID}
            PhenDateTime = resp_station_datastream_WL['value'][0]['Datastreams'][0]['phenomenonTime']
            dates_WL_dict.update({'PhenDateTime': PhenDateTime[(PhenDateTime.find("/")+1):] })
            dates_WL += [dates_WL_dict]

    # Add station's location to WSDS_dict
    WSDS_dict.update({'Coordinates': resp_station_loc['value'][0]['Locations'][0]['location']['coordinates']})

    # Update the WSDS with the new dictionary for the WS
    WSDS += [ WSDS_dict ]

#print("\n ----------------------- ")
#print("WSDS =", WSDS )
#print("--------------------------\n")


#-----------------------------------------------------------------------------------
#   Step 2: Extract real measurements from Sensors at the specific Weather Station
# and
#   Step 3: Create and send the Topic104
#-----------------------------------------------------------------------------------
# Create new Producer instance using provided configuration message (dict data).
#
producer = BusProducer()

# Decorate terminal
print('\033[95m' + "\n***********************")
print("*** CRCL SERVICE v1.0 ***")
print("***********************\n" + '\033[0m')


# Open files to store the query responses
flname_WL = directory + "/" + 'response_Sensors_WL.txt'
outfl_WL = open(flname_WL, 'w')

# Arrays to keep the query responses
response_sensors_WL = []

# List to store all the Topics 104
Topics104 = []

for i, StationID in enumerate(WSDS):

    filt_args={'thing_filt': ['id'], 'dstr_filt': ['name'], 'obs_filt': ['phenomenonTime']}
    sel_vals = {'thing_sel': ['id','name', 'description'],
                'dstr_sel': ['id', 'name', 'phenomenonTime'],
                'obs_sel': ['result', 'phenomenonTime', 'id']}
    ord_vals = ['phenomenonTime']

    # For WL datastream do:
    if StationID['WL'] != None:

        # Find the corresponding PhenomenonTimeDate for WL of the Station
        for k, j in enumerate(dates_WL):
            if j['ID'] == StationID['ID']:
                if len(j) > 1:   #['PhenDateTime'] != None:
                    dt = j['PhenDateTime']
                    filt_vals_WL={'thing_filt': [str(StationID['ID'])], 'dstr_filt': ['Water'], 'obs_filt_vals': dt}

        # Call function to extract the measurement of WL from specific Station
        item_WL = extract_from_WS_Sensors(service_root_URI, SensorThings, sel_vals, ord_vals, filt_args, filt_vals_WL)
        response_sensors_WL.append(item_WL)

        msg_WL = "\n Station ID = " + str(StationID['ID']) + " and Datastream ID = " + str(StationID['WL']) + "\n"
        outfl_WL.write(msg_WL)
        json.dump(item_WL, outfl_WL)
        outfl_WL.write("\n ------------------------------ \n")

        # For each observation CRCL finds its scale
        lenObs = len(item_WL['value'][0]['Datastreams'][0]['Observations'])
        value = []
        for iter_obs in range(0, lenObs):

            value.append(item_WL['value'][0]['Datastreams'][0]['Observations'][iter_obs]['result'])

            # call function to compare the value with alarm thresholds
            if value[iter_obs] > 0.0:
                color_note_WL = compare_value_scale_thresholds(value[iter_obs] , filt_vals_WL['thing_filt'], filt_vals_WL['dstr_filt'], Thresholds_WL)
                meas_ColNote_WL_dict = {'ID': StationID['ID'], 'col': color_note_WL[0], 'note': color_note_WL[1],
                                        'scale': color_note_WL[2], 'note_scale': color_note_WL[3]}
            else: # StationID['WL'] == None:
                meas_ColNote_WL_dict = {'ID': StationID['ID'], 'col': None, 'note': None, 'scale': None, 'note_scale': None}

            meas_ColNote_WL += [meas_ColNote_WL_dict]

    #--------------------------------------------------------------------------------------------
    #  STEP 3: Creates the TOPIC_104_METRIC_REPORT
    #--------------------------------------------------------------------------------------------
    #
    # Create the TOPIC 104 (json format) for each Weather Station and datastream
    # Water Level. The datastream will be consisted of real values
    # retrieved from the sensors at a particular Weather Station and another dataSeries
    # metric which presents the scale.

    # Set variables for the header of the message
    district = "Vicenza"
    #msgIdent = "5433dfde68"

    sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Set variables for the body of the message
    dataStreamGener = "CRCL"
    lang = "it-IT"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the Weather Station
    position = [ StationID['Coordinates'][0], StationID['Coordinates'][1] ]

    #-------------------------------------------------------------------------
    # If the Water Level datastream exist in the specific Weather Station

    # Initialize temporary arrays
    measurement_ID = []
    measurement_TimeStamp = []
    dataSeriesID = []
    dataSeriesName = []
    dsmeas_color = []
    dsmeas_note = []
    yValues = []
    xVals = []

    if StationID['WL'] != None:

        dataStreamName = StationID['WL_name']
        dataStreamDescript = 'Real measurements' + dataStreamName
        dataStreamID = StationID['WL']

        #print("\n dataStreamName = ", dataStreamName, " dataStreamID = ", dataStreamID)

        # Unique message identifier
        msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")

        # Call the class Top104_Metric_Report to create an object data of this class
        data_WL = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                            dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                            lang, dataStreamCategory, dataStreamSubCategory, position)

        # Create the header of the object (message)
        data_WL.create_dictHeader()

        # Create the body and the measurements of the object (message)
        #
        # Extract values from 'response_sensors_WL'
        pos = [j for j, x in enumerate(response_sensors_WL) if x['value'][0]['@iot.id'] == StationID['ID'] ]

        val_measurement_ID = str(response_sensors_WL[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['@iot.id']) + "_1"
        measurement_ID += [ val_measurement_ID ]
        measurement_TimeStamp += [ datetime.now().replace(microsecond=0).isoformat() + 'Z' ]
        xVals += [ response_sensors_WL[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['phenomenonTime'] ]

        # find the position of station and datasteam to the meas_ColNote_WL list
        pos_meas = [j for j, x in enumerate(meas_ColNote_WL) if x['ID'] == StationID['ID'] ]

        # First measurement - real values
        dataSeriesID += ["1"]
        dataSeriesName += ['Real measurement of ' + dataStreamName]

        dsmeas_color += [ meas_ColNote_WL[pos_meas[0]]['col'][0] ]
        dsmeas_note  += [ meas_ColNote_WL[pos_meas[0]]['note'][0] ]

        # Store values to yValues array
        lenObs_yV = len( response_sensors_WL[pos[0]]['value'][0]['Datastreams'][0]['Observations'] )
        yValues = []
        for iter_obs in range(0, lenObs_yV):
            yValues.append( response_sensors_WL[pos[0]]['value'][0]['Datastreams'][0]['Observations'][iter_obs]['result'])

        # Second measurement for the scale
        val_measurement_ID = str(response_sensors_WL[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['@iot.id']) + "_2"
        measurement_ID += [ val_measurement_ID ]
        measurement_TimeStamp += [ response_sensors_WL[pos[0]]['value'][0]['Datastreams'][0]['Observations'][0]['phenomenonTime'] ]

        dataSeriesID += ["2"]
        dataSeriesName += ['Scale']

        yValues += [ meas_ColNote_WL[pos_meas[0]]['scale'] ]

        dsmeas_color += [""]
        dsmeas_note += [ meas_ColNote_WL[pos_meas[0]]['note_scale'][0] ]

        # Set values to the data_WL attributes from temporary arrays
        data_WL.topic_measurementID = measurement_ID
        data_WL.topic_measurementTimeStamp = measurement_TimeStamp
        data_WL.topic_dataSeriesID = dataSeriesID
        data_WL.topic_dataSeriesName = dataSeriesName
        data_WL.topic_xValue = xVals
        data_WL.topic_yValue = yValues

        data_WL.topic_meas_color = dsmeas_color
        data_WL.topic_meas_note = dsmeas_note

        # call class function
        data_WL.create_dictMeasurements()

        # create the body of the object
        data_WL.create_dictBody()

        # create the TOP104_METRIC_REPORT as json
        top104_WL = OrderedDict()
        top104_WL['header']= data_WL.header
        top104_WL['body']= data_WL.body

        # write json (top104_WL) to output file
        flname = directory + "/" + 'TOP104_Water_Level_' + 'WeatherStation_' + str(StationID['ID']) + '.txt'
        with open(flname, 'w') as outfile:
            json.dump(top104_WL, outfile, indent=4)

        Topics104 += [ top104_WL ]

#------------------------------------------------------------------------------------
# STEP 4: Calculate the Overall Crisis Classification Index for all Weather Stations

occi_ws_val = Overall_Crisis_Classification_Index_WeatherStations( meas_ColNote_WL )


# Create the TOP104 for Overall Crisis Classification Index
#
dataStreamGener = "CRCL"
dataStreamName = "Overall Crisis Classification Index"
lang = "it-IT"
dataStreamCategory = "Met"
dataStreamSubCategory = "Flood"
dataStreamID = "3"
dataStreamDescript = "Overall Crisis Classification Index for Vicenza region"

# Position of the specific river section
position = ["11.53885", "45.54497"]

# Set variables for the header of the message
district = "Vicenza"

# Unique message identifier
msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")

sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'
status = "Actual"
actionType = "Update"
scope = "Public"
code = 20190617001

occi_msg = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                lang, dataStreamCategory, dataStreamSubCategory, position)

# create the header of the object
occi_msg.create_dictHeader()

# create the measurements of the object
#
occi_msg.topic_yValue = [occi_ws_val[0]['occi']]
occi_msg.topic_measurementID = ['OCCI_ID_1']
occi_msg.topic_measurementTimeStamp = [sent_dateTime]
occi_msg.topic_dataSeriesID = ['WS_OCCI_ID_1']
occi_msg.topic_dataSeriesName = [occi_ws_val[0]['name']]
occi_msg.topic_xValue = [sent_dateTime]
occi_msg.topic_meas_color = [occi_ws_val[0]['color']]
occi_msg.topic_meas_note = [occi_ws_val[0]['note']]

# call class function
occi_msg.create_dictMeasurements()

# create the body of the object
occi_msg.create_dictBody()

# create the TOP104_METRIC_REPORT as json
top104_occi = OrderedDict()
top104_occi['header']= occi_msg.header
top104_occi['body']= occi_msg.body

# write json (top104_WL) to output file
flname = directory + "/" + 'TOP104_OverallCrisisClassification' + '.txt'
with open(flname, 'w') as outfile:
    json.dump(top104_occi, outfile, indent=4)


Topics104 += [ top104_occi ]


#--------------------------------------------------------------------------------------
# Send messages for the specific Weather Station
if len(Topics104) != 0:
    print('Messages from Weather Station', str(StationID['ID']), 'have been forwarded to logger!')
    for it in range(len(Topics104)):
        producer.send("TOP104_METRIC_REPORT", Topics104[it])
else:
    print('No messages will be forward to logger from Weather Station', str(StationID['ID']), "!" )

# Close files
outfl_WL.close()






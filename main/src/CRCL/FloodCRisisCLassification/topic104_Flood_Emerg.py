import json, time, re
import os, errno
from pathlib import Path
from pandas import read_csv, DataFrame, concat, ExcelWriter
from datetime import datetime, timedelta
from math import pow, ceil
from collections import OrderedDict
# import numpy

from uuid import *

from CRCL.FloodCRisisCLassification.Topic104_Metric_Report import Top104_Metric_Report
from CRCL.FloodCRisisCLassification.Auxiliary_functions import *
from bus.bus_producer import BusProducer

#-----------------------------------------------------------------------------------------------------------------------
#
# Topic 104 for Observed Precipitation Measurements for each Weather Station. The results are going to present to the
# line plots in the Dashboard
#
def topic104FloodEmerg_Precipitation(directory, PRDS_dict, Alarms_RP, flag_status, producer):
    # Set variables for the header of the message
    district = "Vicenza"

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Set variables for the body of the message
    dataStreamGener = "CRCL"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the Weather Station
    position = [PRDS_dict['Coordinates'][0], PRDS_dict['Coordinates'][1]]

    dataStreamName = 'OPRm_Observed Precipitation Measurements'
    dataStreamDescript = 'Precipitation ' + flag_status + ' measurements at ' + PRDS_dict['WS_name']
    dataStreamID = "FLCR_1122_OPRm"
    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")

    # Call the class Top104_Metric_Report to create an object data of this class
    data_PR = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                   dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                   lang, dataStreamCategory, dataStreamSubCategory, position)

    data_PR.create_dictHeader()

    # Create the body and the measurements of the object (message)

    for i in range(len(PRDS_dict['Precipitation'])):

        # Set values to the data_WL attributes from temporary arrays
        # data_PR.topic_measurementID = [str(PRDS_dict['IoT_ID'][i]) + "_" + str(i)]
        data_PR.topic_measurementID = [ str( uuid4() ).replace('-', '') ]

        data_PR.topic_measurementTimeStamp = [datetime.utcnow().replace(microsecond=0).isoformat() + 'Z']

        # topic_dataSeriesID = 'ID Weather Station' + "_" + 'ID_DataStream'
        data_PR.topic_dataSeriesID = [str(PRDS_dict['ID']) + "_" + str(PRDS_dict['DS_ID'])]

        data_PR.topic_dataSeriesName = [PRDS_dict['WS_name']]
        data_PR.topic_xValue = [PRDS_dict['PhenDateTime'][i]]
        data_PR.topic_yValue = [PRDS_dict['Precipitation'][i]]
        data_PR.topic_meas_color = [PRDS_dict['Color'][i]]
        data_PR.topic_meas_note = [PRDS_dict['Note'][i]]
        data_PR.topic_meas_category = [PRDS_dict['Note_Scale'][i]]

         # call class function
        data_PR.create_dictMeasurements_Categ()

        # Create Thresholds
    for it in range(len(Alarms_RP)):
        data_PR.topic_thresh_note = [Alarms_RP[it]['note']]
        data_PR.topic_thresh_color = [Alarms_RP[it]['color']]
        data_PR.topic_thresh_xValue = [Alarms_RP[it]['xValue']]
        data_PR.topic_thresh_yValue = [Alarms_RP[it]['yValue']]

        data_PR.create_dictThresholds()

        # create the body of the object
        data_PR.create_dictBody_withThresh()

         # create the TOP104_METRIC_REPORT as json
        top104_PR = OrderedDict()
        top104_PR['header'] = data_PR.header
        top104_PR['body'] = data_PR.body

        # write json (top104_WL) to output file
        flname = directory + "/" + 'TOP104_Precipitation_' + 'WeatherStation_' + str(PRDS_dict['WS_name']) + '.txt'
        with open(flname, 'w') as outfile:
            json.dump(top104_PR, outfile, indent=4)

     # Send messages to PSAP
    print('Send message: Observed Precipitation topic has been forwarded to logger!')
    producer.send("TOP104_METRIC_REPORT", top104_PR)


#-----------------------------------------------------------------------------------------------------------------------
#
# Topic 104 for Observed Water Level Measurements for each Weather Station. The results are going to present to the
# line plots in the Dashboard
#
def topic104FloodEmerg_WaterLevel(directory, WLDS_dict, Alarms_WL, flag_status, producer):
    # Set variables for the header of the message
    district = "Vicenza"

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Set variables for the body of the message
    dataStreamGener = "CRCL"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the Weather Station
    position = [WLDS_dict['Coordinates'][0], WLDS_dict['Coordinates'][1]]

    dataStreamName = 'OWLm_Observed Water Level Measurements'
    dataStreamDescript = 'Water Level ' + flag_status + ' measurements at ' + WLDS_dict['WS_name']
    dataStreamID = "FLCR_1112_OWLm"
    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")

    # Call the class Top104_Metric_Report to create an object data of this class
    data_WL = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                   dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                   lang, dataStreamCategory, dataStreamSubCategory, position)

    data_WL.create_dictHeader()

    # Create the body and the measurements of the object (message)

    for i in range(len(WLDS_dict['Water_level'])):

        # Set values to the data_WL attributes from temporary arrays
        # data_WL.topic_measurementID = [str(WLDS_dict['IoT_ID'][i]) + "_" + str(i)]
        data_WL.topic_measurementID = [ str( uuid4() ).replace("-", "") ]

        data_WL.topic_measurementTimeStamp = [datetime.utcnow().replace(microsecond=0).isoformat() + 'Z']

        # topic_dataSeriesID = 'ID Weather Station' + "_" + 'ID_DataStream'
        data_WL.topic_dataSeriesID = [ str(WLDS_dict['ID']) + "_" + str(WLDS_dict['DS_ID']) ]

        data_WL.topic_dataSeriesName = [WLDS_dict['WS_name']]
        data_WL.topic_xValue = [WLDS_dict['PhenDateTime'][i]]
        data_WL.topic_yValue = [WLDS_dict['Water_level'][i]]
        data_WL.topic_meas_color = [WLDS_dict['Color'][i]]
        data_WL.topic_meas_note = [WLDS_dict['Note'][i]]
        data_WL.topic_meas_category = [WLDS_dict['Note_Scale'][i]]

        # call class function
        data_WL.create_dictMeasurements_Categ()

    # Create Thresholds
    for it in range(len(Alarms_WL)):
        data_WL.topic_thresh_note = [Alarms_WL[it]['note']]
        data_WL.topic_thresh_color = [Alarms_WL[it]['color']]
        data_WL.topic_thresh_xValue = [Alarms_WL[it]['xValue']]
        data_WL.topic_thresh_yValue = [Alarms_WL[it]['yValue']]

        data_WL.create_dictThresholds()

    # create the body of the object
    data_WL.create_dictBody_withThresh()

    # create the TOP104_METRIC_REPORT as json
    top104_WL = OrderedDict()
    top104_WL['header'] = data_WL.header
    top104_WL['body'] = data_WL.body

    # write json (top104_WL) to output file
    flname = directory + "/" + 'TOP104_Water_Level_' + 'WeatherStation_' + str(WLDS_dict['WS_name']) + '.txt'
    with open(flname, 'w') as outfile:
        json.dump(top104_WL, outfile, indent=4)

     # Send messages to PSAP
    print('Send message: Observed Water Level topic has been forwarded to logger!')
    producer.send("TOP104_METRIC_REPORT", top104_WL)


#-----------------------------------------------------------------------------------------------
#
#   Create a topic104 for the specific WS in order to present it in the PSAP
#
def topic104FloodEmerg_WaterLevel_MAP(directory, WLDS_item, producer):

    # Set variables for the header of the message
    district = "Vicenza"

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Set variables for the body of the message
    dataStreamGener = "CRCL"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the Weather Station (Long/Lat)
    position = [WLDS_item['Coordinates'][0], WLDS_item['Coordinates'][1]]

    # Initialize temporary arrays
    measurement_ID = []
    measurement_TimeStamp = []
    dataSeriesID = []
    dataSeriesName = []
    dsmeas_color = []
    dsmeas_note = []
    yValues = []
    xVals = []

    if WLDS_item['Water_level'] != None:

        dataStreamName = 'OWLm_Observed Water Level Measurement'
        dataStreamDescript = WLDS_item['WS_name'] + ' ,real measurement'
        dataStreamID = "FLCR_1012_OWLm"

        print("\n dataStreamName = ", dataStreamName, " dataStreamID = ", dataStreamID)

        # Unique message identifier
        msgIdent = datetime.utcnow().isoformat().replace(":","").replace("-","").replace(".","MS")

        # Call the class Top104_Metric_Report to create an object data of this class
        data_WL = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

        # Create the header of the object (message)
        data_WL.create_dictHeader()

        # Create the body and the measurements of the object (message)
        #
        # Extract values from 'response_sensors_WL'
        # pos = [j for j, x in enumerate(response_sensors_WL) if x['value'][0]['@iot.id'] == StationID['ID'] ]

        # val_measurement_ID = str( WLDS_item['IoT_ID'][0] ) + "_1"

        val_measurement_ID = str( uuid4() ).replace("-","")
        measurement_ID.append( val_measurement_ID )

        meas_TS = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
        measurement_TimeStamp.append( meas_TS )

        # First measurement - real values
        dataSeriesID.append( str(WLDS_item['ID']) + "_" + str(WLDS_item['DS_ID']) )

        # Create a short names for the dataSeries
        short_name = WLDS_item['WS_name'].replace("Water Level ", "").replace("CAE", "")
        dataSeriesName.append( short_name )

        dsmeas_color.append( WLDS_item['Color'][0] )
        dsmeas_note.append( WLDS_item['Note'][0] )

        # Store values to yValues array
        yValues.append( WLDS_item['Water_level'][0] )
        xVals.append( WLDS_item['PhenDateTime'][0] )

        # Second measurement for the scale
        val_measurement_ID = str( WLDS_item['IoT_ID'][0] ) + "_2"
        measurement_ID.append( val_measurement_ID )
        measurement_TimeStamp.append( meas_TS )

        dataSeriesID.append( str(WLDS_item['ID']) + "_" + str(WLDS_item['DS_ID']) )
        dataSeriesName.append( short_name )  # for 'Scale

        yValues.append( WLDS_item['Scale'][0] )
        xVals.append( WLDS_item['PhenDateTime'][0] )

        dsmeas_color.append( WLDS_item['Color'][0] )
        dsmeas_note.append( WLDS_item['Note_Scale'][0] )

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
        flname = directory + "/" + 'TOP104_Water_Level_To_MAP' + '_' + 'WeatherStation' + '_' + str(WLDS_item['ID']) + \
                 '_' + WLDS_item['WS_name'] + '.txt'

        with open(flname, 'w') as outfile:
            json.dump(top104_WL, outfile, indent=4)

        print('Messages for Water Level over the Weather Station with name: ', WLDS_item['WS_name'], ' have been forwarded to logger!')
        producer.send("TOP104_METRIC_REPORT", top104_WL)

    else:
       print("\n Scale is NONE for the Weather Station with ID ", str(WLDS_item['ID']), ' and name: ', WLDS_item['WS_name'] )


#--------------------------------------------------------------------------------------------------------------------------------
def topic104FloodEmerg_Precipitation_MAP_v2(directory, PRDS_item, producer):

    # Set variables for the header of the message
    district = "Vicenza"

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Set variables for the body of the message
    dataStreamGener = "CRCL"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the Weather Station (Long/Lat)
    position = [PRDS_item['Coordinates'][0], PRDS_item['Coordinates'][1]]

    # Initialize temporary arrays
    measurement_ID = []
    measurement_TimeStamp = []
    dataSeriesID = []
    dataSeriesName = []
    dsmeas_color = []
    dsmeas_note = []
    dsmeas_category = []
    yValues = []
    xVals = []

    if PRDS_item['Precipitation'] != None:

        dataStreamName = 'OPRm_Observed Precipitation Measurement'
        dataStreamDescript = PRDS_item['WS_name'] + ' ,real measurement'
        dataStreamID = "FLCR_1123_OPRm"

        print("\n dataStreamName = ", dataStreamName, " dataStreamID = ", dataStreamID)

        # Unique message identifier
        msgIdent = datetime.utcnow().isoformat().replace(":","").replace("-","").replace(".","MS")

        # Call the class Top104_Metric_Report to create an object data of this class
        data_PR = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

        # Create the header of the object (message)
        data_PR.create_dictHeader()

        # Create the body and the measurements of the object (message)
        #
        # Extract values from 'response_sensors_WL'
        # pos = [j for j, x in enumerate(response_sensors_WL) if x['value'][0]['@iot.id'] == StationID['ID'] ]

        # val_measurement_ID = str( PRDS_item['IoT_ID'][0] ) + "_1"

        val_measurement_ID = str( uuid4() ).replace("-", "")
        measurement_ID.append( val_measurement_ID )

        meas_TS = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
        measurement_TimeStamp.append( meas_TS )

        # First measurement - real values
        dataSeriesID.append( str(PRDS_item['ID']) + "_" + str(PRDS_item['DS_ID']) )

        # Create a short names for the dataSeries
        short_name = PRDS_item['WS_name'].replace("Precipitation", "").replace("CAE", "")
        dataSeriesName.append( short_name )

        pos = len( PRDS_item['Precipitation'] )

        dsmeas_color.append( PRDS_item['Color'][pos-1] )
        dsmeas_note.append( PRDS_item['Note'][pos-1] )
        dsmeas_category.append( PRDS_item['Note_Scale'][pos-1] )

        # Store values to yValues array
        yValues.append( PRDS_item['Precipitation'][pos-1] )
        xVals.append( PRDS_item['PhenDateTime'][pos-1] )

        # Set values to the data_WL attributes from temporary arrays
        data_PR.topic_measurementID = measurement_ID
        data_PR.topic_measurementTimeStamp = measurement_TimeStamp
        data_PR.topic_dataSeriesID = dataSeriesID
        data_PR.topic_dataSeriesName = dataSeriesName
        data_PR.topic_xValue = xVals
        data_PR.topic_yValue = yValues

        data_PR.topic_meas_color = dsmeas_color
        data_PR.topic_meas_note = dsmeas_note
        data_PR.topic_meas_category = dsmeas_category

        # call class function
        data_PR.create_dictMeasurements_Categ()

        # create the body of the object
        data_PR.create_dictBody()

        # create the TOP104_METRIC_REPORT as json
        top104_PR = OrderedDict()
        top104_PR['header']= data_PR.header
        top104_PR['body']= data_PR.body

        # write json (top104_WL) to output file
        flname = directory + "/" + 'TOP104_Precipitation_To_MAP' + '_' + 'WeatherStation' + '_' + str(PRDS_item['ID']) + \
                 '_' + PRDS_item['WS_name'] + '.txt'

        with open(flname, 'w') as outfile:
            json.dump(top104_PR, outfile, indent=4)

        print('Messages for Precipitation over the Weather Station with name: ', PRDS_item['WS_name'], ' have been forwarded to logger!')
        producer.send("TOP104_METRIC_REPORT", top104_PR)

    else:
       print("\n Scale is NONE for the Weather Station with ID ", str(PRDS_item['ID']), ' and name: ', PRDS_item['WS_name'] )


#-------------------------------------------------------------------------------------------------------------------------------
def topic104FloodEmerg_WaterLevel_MAP_v2(directory, WLDS_item, producer):

    # Set variables for the header of the message
    district = "Vicenza"

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Set variables for the body of the message
    dataStreamGener = "CRCL"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the Weather Station (Long/Lat)
    position = [WLDS_item['Coordinates'][0], WLDS_item['Coordinates'][1]]

    # Initialize temporary arrays
    measurement_ID = []
    measurement_TimeStamp = []
    dataSeriesID = []
    dataSeriesName = []
    dsmeas_color = []
    dsmeas_note = []
    dsmeas_category = []
    yValues = []
    xVals = []

    if WLDS_item['Water_level'] != None:

        dataStreamName = 'OWLm_Observed Water Level Measurement'
        dataStreamDescript = WLDS_item['WS_name'] + ' ,real measurement'
        dataStreamID = "FLCR_1012_OWLm"

        print("\n dataStreamName = ", dataStreamName, " dataStreamID = ", dataStreamID)

        # Unique message identifier
        msgIdent = datetime.utcnow().isoformat().replace(":","").replace("-","").replace(".","MS")

        # Call the class Top104_Metric_Report to create an object data of this class
        data_WL = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

        # Create the header of the object (message)
        data_WL.create_dictHeader()

        # Create the body and the measurements of the object (message)
        #
        # Extract values from 'response_sensors_WL'
        # pos = [j for j, x in enumerate(response_sensors_WL) if x['value'][0]['@iot.id'] == StationID['ID'] ]

        # val_measurement_ID = str( WLDS_item['IoT_ID'][0] ) + "_1"

        val_measurement_ID = str( uuid4() ).replace("-","")
        measurement_ID.append( val_measurement_ID )

        meas_TS = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
        measurement_TimeStamp.append( meas_TS )

        # First measurement - real values
        dataSeriesID.append( str(WLDS_item['ID']) + "_" + str(WLDS_item['DS_ID']) )

        # Create a short names for the dataSeries
        short_name = WLDS_item['WS_name'].replace("Water Level ", "").replace("CAE", "")
        dataSeriesName.append( short_name )

        pos = len( WLDS_item['Water_level'] )

        dsmeas_color.append( WLDS_item['Color'][pos-1] )
        dsmeas_note.append( WLDS_item['Note'][pos-1] )
        dsmeas_category.append( WLDS_item['Note_Scale'][pos-1] )

        # Store values to yValues array
        yValues.append( WLDS_item['Water_level'][pos-1] )
        xVals.append( WLDS_item['PhenDateTime'][pos-1] )

        # Set values to the data_WL attributes from temporary arrays
        data_WL.topic_measurementID = measurement_ID
        data_WL.topic_measurementTimeStamp = measurement_TimeStamp
        data_WL.topic_dataSeriesID = dataSeriesID
        data_WL.topic_dataSeriesName = dataSeriesName
        data_WL.topic_xValue = xVals
        data_WL.topic_yValue = yValues

        data_WL.topic_meas_color = dsmeas_color
        data_WL.topic_meas_note = dsmeas_note
        data_WL.topic_meas_category = dsmeas_category

        # call class function
        data_WL.create_dictMeasurements_Categ()

        # create the body of the object
        data_WL.create_dictBody()

        # create the TOP104_METRIC_REPORT as json
        top104_WL = OrderedDict()
        top104_WL['header']= data_WL.header
        top104_WL['body']= data_WL.body

        # write json (top104_WL) to output file
        flname = directory + "/" + 'TOP104_Water_Level_To_MAP' + '_' + 'WeatherStation' + '_' + str(WLDS_item['ID']) + \
                 '_' + WLDS_item['WS_name'] + '.txt'

        with open(flname, 'w') as outfile:
            json.dump(top104_WL, outfile, indent=4)

        print('Messages for Water Level over the Weather Station with name: ', WLDS_item['WS_name'], ' have been forwarded to logger!')
        producer.send("TOP104_METRIC_REPORT", top104_WL)

    else:
       print("\n Scale is NONE for the Weather Station with ID ", str(WLDS_item['ID']), ' and name: ', WLDS_item['WS_name'] )

# ------------------------------------------------------------------------------------------------------------------------------
def topic104FloodEmerg_OverallCrisisLevel( flag_mode, directory, ocl_ws_val, producer ):

    # Create the TOP104 for Overall Crisis Classification Index
    #
    dataStreamGener = "CRCL"
    dataStreamName = "OFLCL_Observed Flood Crisis Level"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"
    dataStreamID = "FLCR_1011_OCL"
    dataStreamDescript = "Overall Crisis Level Index for Vicenza region -- emergency phase"

    # Position of the specific river section
    position = ["11.53885", "45.54497"]

    # Set variables for the header of the message
    district = "Vicenza"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":","").replace("-","").replace(".","MS")

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    ocl_msg = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

    # create the header of the object
    ocl_msg.create_dictHeader()

    # create the measurements of the object
    #
    #ocl_msg.topic_yValue = [ocl_ws_val['ocl']]
    ocl_msg.topic_yValue = [ocl_ws_val['ocl_val']]

    ocl_msg.topic_measurementID = ['OCL_ID_104']
    ocl_msg.topic_measurementTimeStamp = [sent_dateTime]
    ocl_msg.topic_dataSeriesID = ['WS_OCL_ID_104']
    ocl_msg.topic_dataSeriesName = [ocl_ws_val['name']]

    if flag_mode == 'Fake':
        ocl_msg.topic_xValue = [sent_dateTime]
    else:
        ocl_msg.topic_xValue = [ocl_ws_val['PhenDateTime'][0]]  # Pairnei thn teleutaia hmerominia metrishs tou prwtou stathmou

    ocl_msg.topic_meas_color = [ocl_ws_val['color']]
    ocl_msg.topic_meas_note = [ocl_ws_val['note']]
    ocl_msg.topic_meas_category = [ocl_ws_val['note']]

    # call class function
    ocl_msg.create_dictMeasurements_Categ()

    # create the body of the object
    ocl_msg.create_dictBody()

    # create the TOP104_METRIC_REPORT as json
    top104_ocl = OrderedDict()
    top104_ocl['header']= ocl_msg.header
    top104_ocl['body']= ocl_msg.body

    # write json (top104_WL) to output file
    flname = directory + "/" + 'TOP104_OverallCrisisLevel' + '.txt'
    with open(flname, 'w') as outfile:
        json.dump(top104_ocl, outfile, indent=4)

    print('Messages from Overall Crisis Level over Weather Stations have been forwarded to logger!')
    producer.send("TOP104_METRIC_REPORT", top104_ocl)


#-------------------------------------------------------------------------------------------------------------------------------
#
# TOPICS FOR ALGORITHM 3 --- INCIDENT REPORTS FROM MOBILE APP
#
# ---------------------------------------------------------------------------------------------
# Create TOPIC for the RISK
#
def topic104FloodEmerg_Risk( directory, Total_Risk_Assessment, point_of_interest, URI_Time, producer ):

    # Create the TOP104 for Risk Assessment
    #
    dataStreamGener = "CRCL"
    dataStreamName = "OFLCL_Risk Assessment"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"
    dataStreamID = "FLCR_1013_RISK"
    dataStreamDescript = "Risk Assessment and Severity level by Incident Reports for Vicenza region -- emergency phase "

    # Position of the specific river section (long/lat)
    position = [point_of_interest['long'], point_of_interest['lat']]

    # Set variables for the header of the message
    district = "Vicenza"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":","").replace("-","").replace(".","MS")

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    ocl_msg = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

    # create the header of the object
    ocl_msg.create_dictHeader()

    # create the measurements of the object
    #

    ocl_msg.topic_yValue = [ Total_Risk_Assessment['Total_Risk_Assessment'] ]

    ocl_msg.topic_measurementID = [ int(datetime.utcnow().timestamp()) ]
    ocl_msg.topic_measurementTimeStamp = [sent_dateTime]
    ocl_msg.topic_dataSeriesID = ['RA_FLIncRep_100']
    ocl_msg.topic_dataSeriesName = [ 'Risk Assessment from Incident Reports in Municipality' ]
    ocl_msg.topic_xValue = [URI_Time]
    ocl_msg.topic_meas_color = [ "" ]
    ocl_msg.topic_meas_note = [ Total_Risk_Assessment['Total_Severity'] ]
    ocl_msg.topic_meas_category = [ Total_Risk_Assessment['Total_Category']]

    # call class function
    ocl_msg.create_dictMeasurements_Categ()

    # create the body of the object
    ocl_msg.create_dictBody()

    # create the TOP104_METRIC_REPORT as json
    top104_ocl = OrderedDict()
    top104_ocl['header']= ocl_msg.header
    top104_ocl['body']= ocl_msg.body

    # write json (top104_WL) to output file
    flname = directory + "/" + 'TOP104_Risk_Assessment' + '.txt'
    with open(flname, 'w') as outfile:
        json.dump(top104_ocl, outfile, indent=4)

    print('Messages for Risk Assessment by Incident Reports have been forwarded to logger!')
    producer.send("TOP104_METRIC_REPORT", top104_ocl)



# ------------------------------------------------------------------------------------------------------------------
# Create topic 104 for the Distribution of the Severity of incident reports by Severity categories
#
#    "dataStreamID": "FLCR_1014_DIRS"
#    "dataStreamName": "DFLIRS_Distribution of Incident Reports by Severity"
#
def topic104Flood_Traffic_IRs_Dashboard(directory, TR_CLRS, producer):

    # Set variables for the body of the message

    dataStreamGener = "CRCL"
    dataStreamName = "DFLIRS_Distribution of Incident Reports by Severity"
    dataStreamID = "FLCR_1014_DIRS"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    dataStreamDescript = "Distribution of the severity of incident reports in the Municipality."

    # Position of the center of Vicenza region
    position = ["11.53885", "45.54497"]

    # Set variables for the header of the message
    district = "Vicenza"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    traffic_msg = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                   dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                   lang, dataStreamCategory, dataStreamSubCategory, position)

    # create the header of the object
    traffic_msg.create_dictHeader()

    traffic_msg.topic_note = ""

    # create the header of the object
    traffic_msg.create_dictHeader()

    # create the measurements of the object
    #

    ds_id = 'DSev_FLIncRep_100'
    ds_name = 'Incident Reports by Severity in Municipality'

    for i in range( len(TR_CLRS) ):
        traffic_msg.topic_measurementID = [ str(int(datetime.utcnow().timestamp())) + "_" + str(i) ]
        traffic_msg.topic_measurementTimeStamp = [sent_dateTime]
        traffic_msg.topic_yValue = [ int(TR_CLRS[i]['yValue']) ]

        traffic_msg.topic_dataSeriesID = [ ds_id ]
        traffic_msg.topic_dataSeriesName = [ ds_name ]

        traffic_msg.topic_xValue = [""]
        traffic_msg.topic_meas_color = [ TR_CLRS[i]['color'] ]

        # note = {Low, Medium, High, Very High, Unknown}
        traffic_msg.topic_meas_category = [ TR_CLRS[i]['note'] ]

        # category = {Minor, Moderate, Severe, Extreme, Unknown}
        traffic_msg.topic_meas_note = [ TR_CLRS[i]["category"] ]

        # call class function
        traffic_msg.create_dictMeasurements_Categ()

        # create the body of the object
        traffic_msg.create_dictBody()

        # create the TOP104_METRIC_REPORT as json
        top104_TrL_CL = OrderedDict()
        top104_TrL_CL['header'] = traffic_msg.header
        top104_TrL_CL['body'] = traffic_msg.body

    # write json (top104_group_ocl) to output file
    flname = directory + "/" + "TOP104_TrafficLight_Severity_Distribution_Dashboard.txt"

    with open(flname, 'w') as outfile:
        json.dump(top104_TrL_CL, outfile, indent=4)

    # Send messages to PSAP
    print('Send message: Traffic Light topic for Severity Level has been forwarded to logger!')
    producer.send("TOP104_METRIC_REPORT", top104_TrL_CL)


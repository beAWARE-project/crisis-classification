import json, time, re
import os, errno
from pathlib import Path
from pandas import read_csv, DataFrame, concat, ExcelWriter
from datetime import datetime, timedelta
from math import pow, ceil
from collections import OrderedDict

from CRCL.FloodCRisisCLassification.Topic104_Metric_Report import Top104_Metric_Report
from CRCL.FloodCRisisCLassification.Auxiliary_functions import *
from bus.bus_producer import BusProducer

#-------------------------------------------------------------------------------------------
# Create topic 104 for Water Level Measurement and its category for every River Section
#
def topic104FloodIndex(directory, flag_last_run, max_yValues, meas_color, meas_note,
                       max_measurementID, max_measurementTimeStamp, dataSeriesID, dataSeriesName, xVals, dataStreamName,
                       dataStreamID, dataStreamDescript, dates, thresh, riverSections,
                       RiverSect_CountScale, total_top104, counter, mapRS_df, producer):

    # Get the appropriate row of the mapRS_df
    #   mapRS_df['SensorID'] == riverSections["value"][counter]['@iot.id'])

    row_mapRS_df = mapRS_df.index[ mapRS_df['SensorID'] == riverSections["value"][counter]['@iot.id'] ][0]
    #print("row_mapRS_df = ", row_mapRS_df, " ID = ", riverSections["value"][counter]['@iot.id'] )

    # Set variables for the body of the message

    dataStreamGener = "CRCL"
    dataStreamName += ['PWLm_Predicted Water Level Measurement']
    dataStreamID += ['FLCR_1002_PWLm']
    dataStreamName += ['PWLc_Predicted Water Level Category']
    dataStreamID += ['FLCR_1102_PWLc']

    if flag_last_run == True:
        lastRunID = '1' # response_forecast['Datastreams'][0]["properties"]["lastRunId"]
        # dataStreamID = str(lastRunID) + "_" + str(datetime.utcnow().microsecond)
        dataStreamDescript += ["AMICO predictions of water level in the last run with ID:" + str(lastRunID)]
        dataStreamDescript += ["AMICO predictions of water level category in the last run with ID:" + str(lastRunID)]
    else:
        ObsRunID = '1' # response_forecast['Datastreams'][0]['Observations'][0]["parameters"]["runId"]
        # dataStreamID = str(ObsRunID) + "_" + str(datetime.utcnow().microsecond)
        dataStreamDescript += [
            "AMICO predictions of water level in the run with ID:" + str(ObsRunID) ]
                #+ " at dates: " + str(dates[0]) + " to " + str(dates[1])]
        dataStreamDescript += [
            "AMICO predictions of water level category in the run with ID:" + str(ObsRunID) ]
                #+ " at dates: " + str(dates[0]) + " to " + str(dates[1])]

    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the specific river section
    #
    #position = [round(loc_riverSection[0], 5), round(loc_riverSection[1], 5)]
    position = [round(mapRS_df['Long'].iloc[row_mapRS_df], 5),
                round(mapRS_df['Lat'].iloc[row_mapRS_df], 5)]

    # Set variables for the header of the message
    district = "Vicenza"
    # district = "Thessaloniki"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")
    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Call the class Top104_Metric_Report to create an object data of this class
    #
    top104_forecast = []

    for tit in range(0, 2):
        # topic for forecast WL
        data = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID[tit], dataStreamName[tit], dataStreamDescript[tit],
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

        # Record the thresholds for each river Section in the header note
        data.topic_note = "Threshold_1=" + str(thresh[0]) + ", " + "Threshold_2=" + str(
            thresh[1]) + ", " + "Threshold_3=" + str(thresh[2])
        # create the header of the object
        data.create_dictHeader()
        # create the measurements of the object
        #
        # topic for forecast WL
        data.topic_yValue = [max_yValues[tit]]
        data.topic_measurementID = [max_measurementID[tit]]
        data.topic_measurementTimeStamp = [max_measurementTimeStamp[tit]]
        #data.topic_dataSeriesID = [dataSeriesID[tit]]
        #data.topic_dataSeriesName = [dataSeriesName[tit]]
        data.topic_dataSeriesID = [mapRS_df['DataSeriesID'].iloc[row_mapRS_df]]
        data.topic_dataSeriesName = [mapRS_df['DataSeriesName'].iloc[row_mapRS_df]]
        data.topic_xValue = [xVals[tit]]
        data.topic_meas_color = [meas_color[tit]]
        data.topic_meas_note = [meas_note[tit]]

        # call class function
        data.create_dictMeasurements()

        # create the body of the object
        data.create_dictBody()

        # create the TOP104_METRIC_REPORT as json for WL forecasts
        top104_item = OrderedDict()
        top104_item['header'] = data.header
        top104_item['body'] = data.body

        # write json (top104_item) to output file
        if tit == 0:
            flname = directory + "/" + 'TOP104_forecasts_WL_' + riverSections["value"][counter]['name'].replace(" ",
                                                                                                                "") + ".txt"
        else:
            flname = directory + "/" + 'TOP104_forecasts_WL_Category_' + riverSections["value"][counter][
                'name'].replace(" ", "") + ".txt"

        with open(flname, 'w') as outfile:
            json.dump(top104_item, outfile, indent=4)

        top104_forecast += [top104_item]

    if len(top104_forecast) != 0:
        print(
            'Send message: Max Predicted Water Level value and its Category have been forwarded to logger into 2 separate messages!')

        for it in range(len(top104_forecast)):
            producer.send("TOP104_METRIC_REPORT", top104_forecast[it])
            total_top104 = total_top104 + 1
            print( "total_top104 = ", total_top104)
            print("\n ***** TOPIC: ")
            print(top104_forecast[it])
            print("*******\n")
    else:
        print('No messages will be forward to logger!!!')

    return total_top104

#-------------------------------------------------------------------------------------------
# Create topic 104 for Water Level Measurement and its category for every River Section
#
# Rename to topic104FloodIndex_Map from topic104FloodIndex_VER14
#
def topic104FloodIndex_Map(directory, flag_last_run, response_forecast, max_yValues, meas_color, meas_note,
                       max_measurementID, max_measurementTimeStamp, dataSeriesID, dataSeriesName, xVals, dataStreamName,
                       dataStreamID, dataStreamDescript, dates, thresh, riverSections,
                       RiverSect_CountScale, counter, mapRS_df):

    # Get the appropriate row of the mapRS_df
    #   mapRS_df['SensorID'] == riverSections["value"][counter]['@iot.id'])

    row_mapRS_df = mapRS_df.index[ mapRS_df['SensorID'] == riverSections["value"][counter]['@iot.id'] ][0]
    #print("row_mapRS_df = ", row_mapRS_df, " ID = ", riverSections["value"][counter]['@iot.id'] )

    # Set variables for the body of the message

    dataStreamGener = "CRCL"
    dataStreamName += ['PWLm_Predicted Water Level Measurement']
    dataStreamID += ['FLCR_1002_PWLm']
    dataStreamName += ['PWLc_Predicted Water Level Category']
    dataStreamID += ['FLCR_1102_PWLc']

    if flag_last_run == True:
        lastRunID = response_forecast['Datastreams'][0]["properties"]["lastRunId"]
        # dataStreamID = str(lastRunID) + "_" + str(datetime.utcnow().microsecond)
        dataStreamDescript += ["AMICO predictions of water level in the last run with ID:" + str(lastRunID)]
        dataStreamDescript += ["AMICO predictions of water level category in the last run with ID:" + str(lastRunID)]
    else:
        ObsRunID = response_forecast['Datastreams'][0]['Observations'][0]["parameters"]["runId"]
        # dataStreamID = str(ObsRunID) + "_" + str(datetime.utcnow().microsecond)
        dataStreamDescript += [
            "AMICO predictions of water level in the run with ID:" + str(ObsRunID) + " at dates: " + str(
                dates[0]) + " to " + str(dates[1])]
        dataStreamDescript += [
            "AMICO predictions of water level category in the run with ID:" + str(ObsRunID) + " at dates: " + str(
                dates[0]) + " to " + str(dates[1])]

    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the specific river section
    #
    #position = [round(loc_riverSection[0], 5), round(loc_riverSection[1], 5)]
    position = [round(mapRS_df['Long'].iloc[row_mapRS_df], 5),
                round(mapRS_df['Lat'].iloc[row_mapRS_df], 5)]

    # Set variables for the header of the message
    district = "Vicenza"
    # district = "Thessaloniki"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")
    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Call the class Top104_Metric_Report to create an object data of this class
    #
    top104_forecast = []

    for tit in range(0, 2):
        # topic for forecast WL
        data = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID[tit], dataStreamName[tit], dataStreamDescript[tit],
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

        # Record the thresholds for each river Section in the header note
        # data.topic_note = "Threshold_1=" + str(thresh[0]['yValue']) + ", " + \
        #                   "Threshold_2=" + str(thresh[1]['yValue']) + ", " + \
        #                   "Threshold_3=" + str(thresh[2]['yValue'])

        #NEW
        data.topic_note = ""

        # create the header of the object
        data.create_dictHeader()
        # create the measurements of the object
        #
        # topic for forecast WL
        data.topic_yValue = [max_yValues[tit]]
        data.topic_measurementID = [max_measurementID[tit]]
        data.topic_measurementTimeStamp = [max_measurementTimeStamp[tit]]
        #data.topic_dataSeriesID = [dataSeriesID[tit]]
        #data.topic_dataSeriesName = [dataSeriesName[tit]]
        data.topic_dataSeriesID = [mapRS_df['DataSeriesID'].iloc[row_mapRS_df]]
        data.topic_dataSeriesName = [mapRS_df['DataSeriesName'].iloc[row_mapRS_df]]
        data.topic_xValue = [xVals[tit]]
        data.topic_meas_color = [meas_color[tit]]
        data.topic_meas_note = [meas_note[tit]]

        # call class function
        data.create_dictMeasurements_Categ()

        # create the body of the object
        data.create_dictBody()

        # create the TOP104_METRIC_REPORT as json for WL forecasts
        top104_item = OrderedDict()
        top104_item['header'] = data.header
        top104_item['body'] = data.body

        # write json (top104_item) to output file
        if tit == 0:
            flname = directory + "/" + 'TOP104_forecasts_WL_Map_' + riverSections["value"][counter]['name'].replace(" ","") + ".txt"
        else:
            flname = directory + "/" + 'TOP104_forecasts_WL_Category_Map_' + riverSections["value"][counter]['name'].replace(" ", "") + ".txt"

        with open(flname, 'w') as outfile:
            json.dump(top104_item, outfile, indent=4)

        top104_forecast += [top104_item]

    return top104_forecast

#---------------------------------------------------------------------------------------------------

def topic104FloodIndex_Map_v2(directory, flag_last_run, max_yValues, meas_color, meas_note, meas_category,
                       max_measurementID, max_measurementTimeStamp, dataSeriesID, dataSeriesName, xVals, dataStreamName,
                       dataStreamID, dataStreamDescript, dates, thresh, riverSections,
                       RiverSect_CountScale, counter, mapRS_df):

    # Get the appropriate row of the mapRS_df
    #   mapRS_df['SensorID'] == riverSections["value"][counter]['@iot.id'])

    row_mapRS_df = mapRS_df.index[ mapRS_df['SensorID'] == riverSections["value"][counter]['@iot.id'] ][0]
    #print("row_mapRS_df = ", row_mapRS_df, " ID = ", riverSections["value"][counter]['@iot.id'] )

    # Set variables for the body of the message

    dataStreamGener = "CRCL"
    dataStreamName += ['PWLm_Predicted Water Level Measurement']
    dataStreamID += ['FLCR_1002_PWLm']

    if flag_last_run == True:
        lastRunID = '1' # response_forecast['Datastreams'][0]["properties"]["lastRunId"]
        # dataStreamID = str(lastRunID) + "_" + str(datetime.utcnow().microsecond)
        dataStreamDescript += ["AMICO predictions of water level in the last run with ID:" + str(lastRunID)]

    else:
        ObsRunID = '1' # response_forecast['Datastreams'][0]['Observations'][0]["parameters"]["runId"]
        # dataStreamID = str(ObsRunID) + "_" + str(datetime.utcnow().microsecond)
        dataStreamDescript += [
            "AMICO predictions of water level in the run with ID:" + str(ObsRunID) ]
                    #+ " at dates: " + str(dates[0]) + " to " + str(dates[1])]

    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the specific river section
    #
    #position = [round(loc_riverSection[0], 5), round(loc_riverSection[1], 5)]
    position = [round(mapRS_df['Long'].iloc[row_mapRS_df], 5),
                round(mapRS_df['Lat'].iloc[row_mapRS_df], 5)]

    # Set variables for the header of the message
    district = "Vicenza"
    # district = "Thessaloniki"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")
    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Call the class Top104_Metric_Report to create an object data of this class
    #
    top104_forecast = []

    # topic for forecast WL
    data = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                dataStreamGener, dataStreamID[0], dataStreamName[0], dataStreamDescript[0],
                                lang, dataStreamCategory, dataStreamSubCategory, position)

    # Record the thresholds for each river Section in the header note
    # data.topic_note = "Threshold_1=" + str(thresh[0]['yValue']) + ", " + \
    #                   "Threshold_2=" + str(thresh[1]['yValue']) + ", " + \
    #                   "Threshold_3=" + str(thresh[2]['yValue'])

    #NEW value
    data.topic_note = ""

    # create the header of the object
    data.create_dictHeader()
    # create the measurements of the object
    #
    # topic for forecast WL
    data.topic_yValue = [max_yValues[0]]
    data.topic_measurementID = [max_measurementID[0]]
    data.topic_measurementTimeStamp = [max_measurementTimeStamp[0]]
    #data.topic_dataSeriesID = [dataSeriesID]
    #data.topic_dataSeriesName = [dataSeriesName]
    data.topic_dataSeriesID = [mapRS_df['DataSeriesID'].iloc[row_mapRS_df]]
    data.topic_dataSeriesName = [mapRS_df['DataSeriesName'].iloc[row_mapRS_df]]
    data.topic_xValue = [xVals[0]]
    data.topic_meas_color = [meas_color[0]]
    data.topic_meas_note = [meas_note[0]]
    data.topic_meas_category = [meas_category[0]]

    # call class function
    data.create_dictMeasurements_Categ()

    # create the body of the object
    data.create_dictBody()

    # create the TOP104_METRIC_REPORT as json for WL forecasts
    top104_item = OrderedDict()
    top104_item['header'] = data.header
    top104_item['body'] = data.body

    flname = directory + "/" + 'TOP104_forecasts_WL_Map_' + riverSections["value"][counter]['name'].replace(" ","") + ".txt"

    with open(flname, 'w') as outfile:
        json.dump(top104_item, outfile, indent=4)

    top104_forecast += [top104_item]

    return top104_forecast




#------------------------------------------------------------------------------------------------
# Create topic 104 for Water Level Measurement and its category for every CRITICAL River Section
#   DataStream name:
#
def topic104FloodIndex_critical(directory, flag_last_run, response_forecast, max_yValues, meas_color, meas_note,
                                max_measurementID, max_measurementTimeStamp, dataSeriesID, dataSeriesName, xVals, dataStreamName,
                                dataStreamID, dataStreamDescript, dates, thresh, riverSections,
                                RiverSect_CountScale, counter, mapRS_df):

    # Get the appropriate row of the mapRS_df
    #   mapRS_df['SensorID'] == riverSections["value"][counter]['@iot.id'])

    row_mapRS_df = mapRS_df.index[ mapRS_df['SensorID'] == riverSections["value"][counter]['@iot.id'] ][0]
    #print("row_mapRS_df = ", row_mapRS_df, " ID = ", riverSections["value"][counter]['@iot.id'] )

    # Set variables for the body of the message

    dataStreamGener = "CRCL"
    dataStreamName += ['PWLm_Predicted Water Level for Critical Sections']
    dataStreamID += ['FLCR_1032_CPWLm']
    dataStreamName += ['PWLc_Predicted Water Level Category for Critical Sections']
    dataStreamID += ['FLCR_1132_CPWLc']

    if flag_last_run == True:
        lastRunID = response_forecast['Datastreams'][0]["properties"]["lastRunId"]
        # dataStreamID = str(lastRunID) + "_" + str(datetime.utcnow().microsecond)
        dataStreamDescript += ["AMICO predictions of water level in the last run with ID:" + str(lastRunID)]
        dataStreamDescript += ["AMICO predictions of water level category in the last run with ID:" + str(lastRunID)]
    else:
        ObsRunID = response_forecast['Datastreams'][0]['Observations'][0]["parameters"]["runId"]
        # dataStreamID = str(ObsRunID) + "_" + str(datetime.utcnow().microsecond)
        dataStreamDescript += [ "AMICO predictions of water level in the run with ID:" + str(ObsRunID) ]
            #+ " at dates: " + str(dates[0]) + " to " + str(dates[1])]
        dataStreamDescript += [
            "AMICO predictions of water level category in the run with ID:" + str(ObsRunID) ]
                #+ " at dates: " + str(dates[0]) + " to " + str(dates[1])]

    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the specific river section
    #
    #position = [round(loc_riverSection[0], 5), round(loc_riverSection[1], 5)]
    position = [round(mapRS_df['Long'].iloc[row_mapRS_df], 5),
                round(mapRS_df['Lat'].iloc[row_mapRS_df], 5)]

    # Set variables for the header of the message
    district = "Vicenza"
    # district = "Thessaloniki"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")
    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Call the class Top104_Metric_Report to create an object data of this class
    #
    top104_forecast_critical = []

    for tit in range(0, 2):
        # topic for forecast WL
        data = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID[tit], dataStreamName[tit], dataStreamDescript[tit],
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

        # Record the thresholds for each river Section in the header note
        data.topic_note = "Threshold_1=" + str(thresh[0]) + ", " + "Threshold_2=" + str(
            thresh[1]) + ", " + "Threshold_3=" + str(thresh[2])
        # create the header of the object
        data.create_dictHeader()
        # create the measurements of the object
        #
        # topic for forecast WL
        data.topic_yValue = [max_yValues[tit]]
        data.topic_measurementID = [max_measurementID[tit]]
        data.topic_measurementTimeStamp = [xVals[tit]]
        #data.topic_dataSeriesID = [dataSeriesID[tit]]
        #data.topic_dataSeriesName = [dataSeriesName[tit]]
        data.topic_dataSeriesID = [mapRS_df['DataSeriesID'].iloc[row_mapRS_df]]
        data.topic_dataSeriesName = [mapRS_df['DataSeriesName'].iloc[row_mapRS_df]]
        data.topic_xValue = [xVals[tit]]
        data.topic_meas_color = [meas_color[tit]]
        data.topic_meas_note = [meas_note[tit]]

        # call class function
        data.create_dictMeasurements()

        # create the body of the object
        data.create_dictBody()

        # create the TOP104_METRIC_REPORT as json for WL forecasts
        top104_item = OrderedDict()
        top104_item['header'] = data.header
        top104_item['body'] = data.body

        # write json (top104_item) to output file
        if tit == 0:
            flname = directory + "/" + 'CRITICAL_TOP104_forecasts_WL' + '_' + riverSections["value"][counter]['name'].replace(" ","") + ".txt"
        else:
            flname = directory + "/" + 'CRITICAL_TOP104_forecasts_WL_Category' + '_' + riverSections["value"][counter][
                'name'].replace(" ", "") + ".txt"

        with open(flname, 'w') as outfile:
            json.dump(top104_item, outfile, indent=4)

        top104_forecast_critical += [top104_item]

    return top104_forecast_critical


#------------------------------------------------------------------------------------
# Create Topic104 for the Predicted Flood Crisis Level per group of river sections
# and the whole region of interest
#
def topic104FloodOverall(directory, RiverSect_CountScale, OCL, total_top104_index, producer):

    # Set variables for the body of the message

    dataStreamGener = "CRCL"
    dataStreamName = "PFLCL_Predicted Flood Crisis Level for each River Reach"
    dataStreamID = "FLCR_1021_PCL"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Create topics for each group of river sections
    for it in range(len(OCL) - 1):
        grID = it + 1

        #dataStreamID = "1021" #+ str(grID)
        # dataStreamDescript = RiverSect_CountScale[it]['descr']

        dataStreamDescript = "Predicted Flood Crisis Level in the " + RiverSect_CountScale[it]['name']

        # Position of the center of group
        position = [round(RiverSect_CountScale[it]['group_center_pos'][0], 5),
                    round(RiverSect_CountScale[it]['group_center_pos'][1], 5)]

        # Set variables for the header of the message
        district = "Vicenza"
        # district = "Thessaloniki"

        # Unique message identifier
        msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")

        sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
        status = "Actual"
        actionType = "Update"
        scope = "Public"
        code = 20190617001

        group_ocl_msg = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                             dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                             lang, dataStreamCategory, dataStreamSubCategory, position)

        # create the header of the object
        group_ocl_msg.create_dictHeader()

        # create the measurements of the object
        #
        #group_ocl_msg.topic_yValue = [OCL[it]['ocl']]
        group_ocl_msg.topic_yValue = [OCL[it]['ocl_val']]

        group_ocl_msg.topic_measurementID = ['OCL_ID_1001' + str(it)]
        group_ocl_msg.topic_measurementTimeStamp = [sent_dateTime]
        group_ocl_msg.topic_dataSeriesID = ['RS_OCL_ID_1001' + str(it)]
        group_ocl_msg.topic_dataSeriesName = [OCL[it]['name']]
        group_ocl_msg.topic_xValue = [sent_dateTime]
        group_ocl_msg.topic_meas_color = [OCL[it]['color']]
        group_ocl_msg.topic_meas_note = [OCL[it]['note']]

        group_ocl_msg.topic_meas_category = [OCL[it]['category']]

        # call class function
        group_ocl_msg.create_dictMeasurements_Categ()

        # create the body of the object
        group_ocl_msg.create_dictBody()

        # create the TOP104_METRIC_REPORT as json
        top104_group_ocl = OrderedDict()
        top104_group_ocl['header'] = group_ocl_msg.header
        top104_group_ocl['body'] = group_ocl_msg.body

        # write json (top104_group_ocl) to output file
        flname = directory + "/" + "TOP104_PreAlert_Overall_Crisis_Level_Group_" + str(grID) + ".txt"

        with open(flname, 'w') as outfile:
            json.dump(top104_group_ocl, outfile, indent=4)

        # Send messages to PSAP
        print('Send message: Overall Crisis Level has been forwarded to logger!')
        producer.send("TOP104_METRIC_REPORT", top104_group_ocl)

        total_top104_index = total_top104_index + 1

    # ----------------------------------------------
    # Create topic for the whole Region of Interest

    dataStreamName = "PFLCL_Predicted Flood Crisis Level Overall"
    dataStreamID = "FLCR_1001_PCL"
    dataStreamDescript = "Estimation of the Flood Crisis Level in the pre-emergency phase for all rivers in the Municipality/ Tutti I Corsi d’acqua nel Comune"

    # Position of the center of Vicenza region
    position = ["11.53885", "45.54497"]

    # Set variables for the header of the message
    district = "Vicenza"
    # district = "Thessaloniki"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")

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
    len_ocl = len(OCL)

    #ocl_msg.topic_yValue = [OCL[len_ocl - 1]['ocl']]
    ocl_msg.topic_yValue = [OCL[len_ocl - 1]['ocl_val']]

    ocl_msg.topic_measurementID = ['OCL_ID_1001']
    ocl_msg.topic_measurementTimeStamp = [sent_dateTime]
    ocl_msg.topic_dataSeriesID = ['RS_OCL_ID_1001']
    ocl_msg.topic_dataSeriesName = [OCL[len_ocl - 1]['name']]
    ocl_msg.topic_xValue = [sent_dateTime]
    ocl_msg.topic_meas_color = [OCL[len_ocl - 1]['color']]
    ocl_msg.topic_meas_note = [OCL[len_ocl - 1]['note']]

    ocl_msg.topic_meas_category = [OCL[len_ocl - 1]['category']]

    # call class function
    ocl_msg.create_dictMeasurements_Categ()

    # create the body of the object
    ocl_msg.create_dictBody()

    # create the TOP104_METRIC_REPORT as json
    top104_ocl = OrderedDict()
    top104_ocl['header'] = ocl_msg.header
    top104_ocl['body'] = ocl_msg.body

    # write json (top104_ocl) to output file
    flname = directory + "/" + "TOP104_PreAlert_Overall_Crisis_Level.txt"
    with open(flname, 'w') as outfile:
        json.dump(top104_ocl, outfile, indent=4)

    # Send messages to PSAP
    print('Send message: Overall Crisis Level has been forwarded to logger!')
    producer.send("TOP104_METRIC_REPORT", top104_ocl)

    total_top104_index = total_top104_index + 1

    return total_top104_index


#------------------------------------------------------------------------------------------------
# Create topic 104 for Water Level Measurement for every CRITICAL River Section.
# The Topic104 message contains the forecasting values of WL per hour as elements
# at the measurement. Also, it contains the Alarm Thresholds for the specific critical RS.
#
#   "dataStreamID": "FLCR_1032_CPWLm",
#   "dataStreamName": "PWLm_Predicted Water Level for Critical Sections"
#
def topic104FloodIndex_critical_linePlot(directory, flag_last_run, comp_RS_df, thresh,
                                         riverSections, measurementTimeStamp, dates, counter, mapRS_df):

    # Get the appropriate row of the mapRS_df
    #   mapRS_df['SensorID'] == riverSections["value"][counter]['@iot.id'])

    row_mapRS_df = mapRS_df.index[ mapRS_df['SensorID'] == riverSections["value"][counter]['@iot.id'] ][0]
    #print("row_mapRS_df = ", row_mapRS_df, " ID = ", riverSections["value"][counter]['@iot.id'] )

    # Set variables for the body of the message

    dataStreamGener = "CRCL"
    dataStreamName = 'PWLm_Predicted Water Level per hour for Critical Sections'
    dataStreamID = 'FLCR_1032_CPWLm'

    if flag_last_run == True:
        lastRunID = '1' #response_forecast['Datastreams'][0]["properties"]["lastRunId"]
        # dataStreamID = str(lastRunID) + "_" + str(datetime.utcnow().microsecond)
        dataStreamDescript = "AMICO predictions of water level in the last run with ID:" + str(lastRunID)
    else:
        ObsRunID = '1' #response_forecast['Datastreams'][0]['Observations'][0]["parameters"]["runId"]
        # dataStreamID = str(ObsRunID) + "_" + str(datetime.utcnow().microsecond)
        dataStreamDescript = "AMICO predictions of water level in the run with ID:" + str(ObsRunID)
                             #+ " at dates: " + str(dates[0]) + " to " + str(dates[1])

    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Position of the specific river section
    #
    #position = [round(loc_riverSection[0], 5), round(loc_riverSection[1], 5)]
    position = [round(mapRS_df['Long'].iloc[row_mapRS_df], 5),
                round(mapRS_df['Lat'].iloc[row_mapRS_df], 5)]

    # Set variables for the header of the message
    district = "Vicenza"
    # district = "Thessaloniki"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")
    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    # Call the class Top104_Metric_Report to create an object data of this class
    #
    top104_forecast_critical = []

    data = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                lang, dataStreamCategory, dataStreamSubCategory, position)

    # Record the thresholds for each river Section in the header note
    #data.topic_note = "Threshold_1=" + str(thresh[0]) + ", " + "Threshold_2=" + str(thresh[1]) + ", " + "Threshold_3=" + str(thresh[2])

    # NEW
    data.topic_note = ""

    # create the header of the object
    data.create_dictHeader()

    # create the measurements of the object
    #
    # topic for forecast WL
    for i in range( comp_RS_df.shape[0] ):

        data.topic_yValue = [ comp_RS_df['result'][i] ]

        # data.topic_measurementID = [ int(comp_RS_df['MeasurementID'][i]) ]
        td = datetime.utcnow()
        data.topic_measurementID = [ str(int(datetime( td.year, td.month, td.day, td.hour, td.minute, td.second).timestamp())) + "_" + str(i)  ]

        data.topic_measurementTimeStamp = [ measurementTimeStamp[0] ]
        data.topic_dataSeriesID = [mapRS_df['DataSeriesID'].iloc[row_mapRS_df]]
        data.topic_dataSeriesName = [mapRS_df['DataSeriesName'].iloc[row_mapRS_df]]

        data.topic_xValue = [ comp_RS_df['phenomenonTime'][i] ]
        data.topic_meas_color = [ comp_RS_df['Measurement_Color'][i] ]
        data.topic_meas_note = [ comp_RS_df['Measurement_Note'][i] ]

        data.topic_meas_category = [ comp_RS_df['Scale_Note'][i] ]

        # call class function
        data.create_dictMeasurements_Categ()

    # Create Thresholds
    for it in range(len(thresh)):

        data.topic_thresh_note = [ thresh[it]['note'] ]
        data.topic_thresh_color = [ thresh[it]['color'] ]
        data.topic_thresh_xValue = [ thresh[it]['xValue'] ]
        data.topic_thresh_yValue = [ thresh[it]['yValue'] ]

        data.create_dictThresholds()

    # create the body of the object
    data.create_dictBody_withThresh()

    # create the TOP104_METRIC_REPORT as json for WL forecasts
    top104_item = OrderedDict()
    top104_item['header'] = data.header
    top104_item['body'] = data.body

    # write json (top104_item) to output file
    flname = directory + "/" + 'CRITICAL_TOP104_forecasts_WL_Dashboard' + '_' + riverSections["value"][counter]['name'].replace(" ","") + ".txt"

    with open(flname, 'w') as outfile:
        json.dump(top104_item, outfile, indent=4)

    top104_forecast_critical += [top104_item]

    return top104_forecast_critical

#------------------------------------------------------------------------------------------------------------------
# Create topic 104 for the Distribution of Predicted Flood Crisis Level at all River Sections in the Municipality
#
#    "dataStreamID": "FLCR_1031_DPCL"
#    "dataStreamName": "DPFLCL_Distribution of Predicted Flood Crisis Level"
#
def topic104Flood_Traffic_Dashboard(directory, TR_CLRS, producer):

    top104_TrL_CL_counter = 0

    # Set variables for the body of the message

    dataStreamGener = "CRCL"
    dataStreamName = "DPFLCL_Distribution of Predicted Flood Crisis Level"
    dataStreamID = "FLCR_1031_DPCL"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    dataStreamDescript = "Distribution of Predicted Flood Crisis Level in the Municipality over all River Sections"

    # Position of the center of Vicenza region
    position = ["11.53885", "45.54497"]

    # Set variables for the header of the message
    district = "Vicenza"
    # district = "Thessaloniki"

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
    for i in range( len(TR_CLRS) ):
        traffic_msg.topic_measurementID = [ str(int(datetime.utcnow().timestamp())) + "_" + str(i) ]
        traffic_msg.topic_measurementTimeStamp = [sent_dateTime]
        traffic_msg.topic_yValue = [ int(TR_CLRS[i]['TrL_value']) ]

        traffic_msg.topic_dataSeriesID = [ TR_CLRS[i]['TrL_ID'] ]
        traffic_msg.topic_dataSeriesName = [ TR_CLRS[i]['TrL_name'] ]

        traffic_msg.topic_xValue = [""]
        traffic_msg.topic_meas_color = [ TR_CLRS[i]['TrL_color'] ]

        traffic_msg.topic_meas_category = [TR_CLRS[i]['TrL_note'] ]

        traffic_msg.topic_meas_note = [""]

        # call class function
        traffic_msg.create_dictMeasurements_Categ()

    # create the body of the object
    traffic_msg.create_dictBody()

    # create the TOP104_METRIC_REPORT as json
    top104_TrL_CL = OrderedDict()
    top104_TrL_CL['header'] = traffic_msg.header
    top104_TrL_CL['body'] = traffic_msg.body

    # write json (top104_group_ocl) to output file
    flname = directory + "/" + "TOP104_TrafficLight_CrisisLevel_Dashboard.txt"

    with open(flname, 'w') as outfile:
        json.dump(top104_TrL_CL, outfile, indent=4)

    # Send messages to PSAP
    print('Send message: Traffic Light topic for Overall Crisis Level has been forwarded to logger!')
    producer.send("TOP104_METRIC_REPORT", top104_TrL_CL)

    top104_TrL_CL_counter = top104_TrL_CL_counter + 1

    return( top104_TrL_CL_counter )


#------------------------------------------------------------------------------------------------------------------
# Create topic 104 for the Distribution of Predicted Flood Crisis Level in each one of the River Reaches
#
#    "dataStreamID": "FLCR_1041_DPCL"
#    "dataStreamName": "DPFLCL_Distribution of Predicted Flood Crisis Level for each River Reach"
#
def topic104Flood_PieChart_Dashboard(directory, RS_CS, producer):

    top104_Pie_counter = 0

    for i in range(len(RS_CS)):

        categories_pie = ['Low', 'Medium', 'High', 'Very High']
        # colors_pie = ["#00FF00", "#FFFF00", "#FFA500", "#FF0000"]
        colors_pie = ['#31A34F', '#F8E71C', '#E68431', '#AA2050' ]

        # Set variables for the body of the message
        dataStreamGener = "CRCL"
        dataStreamName = "DPFLCL_Distribution of Predicted Flood Crisis Level for each River Reach "
        dataStreamID = "FLCR_1041_DPCL"
        lang = "en-US"
        dataStreamCategory = "Met"
        dataStreamSubCategory = "Flood"

        dataStreamDescript = "Distribution of Predicted Flood Crisis Level in " + RS_CS[i]['name']

        # Position of the center of the position of the River Reach
        position = RS_CS[i]['group_center_pos']

        # Set variables for the header of the message
        district = "Vicenza"

        # Unique message identifier
        msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")

        sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
        status = "Actual"
        actionType = "Update"
        scope = "Public"
        code = 20190617001

        pie_msg = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                       dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                       lang, dataStreamCategory, dataStreamSubCategory, position)

        # create the header of the object
        pie_msg.create_dictHeader()

        pie_msg.topic_note = ""

        # create the header of the object
        pie_msg.create_dictHeader()

        # create the measurements of the object
        #
        for j in range( len( RS_CS[i]['count'] ) ):
            pie_msg.topic_measurementID = [ str(int(datetime.utcnow().timestamp())) + "_" + str(j) ]
            pie_msg.topic_measurementTimeStamp = [sent_dateTime]
            pie_msg.topic_yValue = [ int( RS_CS[i]['count'][j] ) ]

            pie_msg.topic_dataSeriesID = [ 'RS_DPCL_ID_1001' + str(RS_CS[i]['id']-1) ]
            pie_msg.topic_dataSeriesName = [ RS_CS[i]['name'] ]

            pie_msg.topic_xValue = [""]
            pie_msg.topic_meas_color = [ colors_pie[j] ]
            pie_msg.topic_meas_note = [""]

            pie_msg.topic_meas_category = [  categories_pie[j] ]

            # call class function
            pie_msg.create_dictMeasurements_Categ()

            # create the body of the object
            pie_msg.create_dictBody()

        # create the TOP104_METRIC_REPORT as json
        top104_Pie_CL = OrderedDict()
        top104_Pie_CL['header'] = pie_msg.header
        top104_Pie_CL['body'] = pie_msg.body

        # write json (top104_group_ocl) to output file
        flname = directory + "/" + "TOP104_PieChart_CrisisLevel_Dashboard" + RS_CS[i]['name'] + ".txt"

        with open(flname, 'w') as outfile:
            json.dump(top104_Pie_CL, outfile, indent=4)

        # Send messages to PSAP
        print("Send message: Pie Chart topic for ", RS_CS[i]['name'], " has been forwarded to logger!")
        producer.send("TOP104_METRIC_REPORT", top104_Pie_CL)

        top104_Pie_counter = top104_Pie_counter + 1

    return( top104_Pie_counter )
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
def topic104FloodIndex(directory, flag_last_run, response_forecast, max_yValues, meas_color, meas_note,
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
def topic104FloodIndex_VER14(directory, flag_last_run, response_forecast, max_yValues, meas_color, meas_note,
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
    dataStreamName = "PFLCL_Predicted Flood Crisis Level by Group of River Sections"
    dataStreamID = "FLCR_1021_PCL"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Flood"

    # Create topics for each group of river sections
    for it in range(len(OCL) - 1):
        grID = it + 1

        #dataStreamID = "1021" #+ str(grID)
        #dataStreamDescript = "Estimation of the Flood Crisis Level in the pre-emergency phase for the " + \
        #                     RiverSect_CountScale[it]['name'] + " of river sections"

        dataStreamDescript = RiverSect_CountScale[it]['descr']

        # Position of the center of group
        position = [round(RiverSect_CountScale[it]['group_center_pos'][0], 5),
                    round(RiverSect_CountScale[it]['group_center_pos'][1], 5)]

        # Set variables for the header of the message
        district = "Vicenza"

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

        # call class function
        group_ocl_msg.create_dictMeasurements()

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

    # call class function
    ocl_msg.create_dictMeasurements()

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


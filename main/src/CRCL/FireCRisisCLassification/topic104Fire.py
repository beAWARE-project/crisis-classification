# Created Date: 06/06/2018
# Modified Date: 12/09/2018
#
#   Main function to create TOPICS 104 for the fire pilot
#

from CRCL.FireCRisisCLassification.Topic104_Metric_Report import Top104_Metric_Report
from datetime import datetime, timedelta
from bus.bus_producer import BusProducer

from pathlib import Path
from collections import OrderedDict
import json, time, re


def topic104Fire(directory, df, df_max, FOCL_list, categories, interp_method):

    counter_topics = 0

    print(" df_1st = ", df['Fire_Danger'].unique())
    print(" df_max = ", df_max['Fire_Danger'].unique())

    producer = BusProducer()

    # Decorate terminal
    print('\033[95m' + "\n***********************")
    print("*** CRCL SERVICE v1.0 ***")
    print("***********************\n" + '\033[0m')

    # ----------------------------------------------------------------------------------
    # B) PFWI_Predicted Fire Weather Index --- 1st FWI per point
    for i in range(df.shape[0]):

        if df['Fire_Danger'].iloc[i] in categories[2:]:
            dataStreamGener = "CRCL"
            dataStreamName = "PFWI_Predicted Fire Weather Index"
            dataStreamID = 'FRCR_2002_PFWI'
            dataStreamDescript = "Canadian Fire Weather Index (FWI) per point of interst forwarding the first measurement which exceeds the Moderate Fire Danger category"
            lang = "en-US"
            dataStreamCategory = "Fire"
            dataStreamSubCategory = ""

            # Position (long/lat)
            LON_V = df['Long'].iloc[i]
            LAT_V = df['Lat'].iloc[i]
            position = [round(LON_V, 5), round(LAT_V, 5)]

            # Set variables for the header of the message
            district = "Valencia"

            # Unique message identifier
            msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")

            sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
            status = "Actual"
            actionType = "Update"
            scope = "Public"
            code = 20190617001

            # Call the class Top104_Metric_Report to create an object data of this class
            fwi_msg = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                           dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                           lang, dataStreamCategory, dataStreamSubCategory, position)

            # Record the thresholds for each weather indicator in the header note
            fwi_msg.topic_note = " "

            # create the header of the object
            fwi_msg.create_dictHeader()

            # create the measurements of the object
            #
            col_name = 'FWI' + "_" + interp_method
            fwi_msg.topic_yValue = [round(df.loc[i, col_name], 3)]

            fwi_msg.topic_measurementID = [int(round(time.time() * 1000))]

            # Measurement TimeStamp is equal with the Date/Time in which the predictions take place
            fwi_msg.topic_measurementTimeStamp = [str(df['Date'].iloc[0]) + 'Z']

            fwi_msg.topic_dataSeriesID = [str(int(abs(position[1]) * 100)) + str(int(abs(position[0]) * 100))]
            fwi_msg.topic_dataSeriesName = [df['Name'].iloc[i]]

            fwi_msg.topic_xValue = [str(df['Date'].iloc[i]) + 'Z']
            fwi_msg.topic_meas_color = [df['Color'].iloc[i]]
            fwi_msg.topic_meas_note = [df['Fire_Danger'].iloc[i]]

            # call class function
            fwi_msg.create_dictMeasurements()

            # create the body of the object
            fwi_msg.create_dictBody()

            # create the TOP104_METRIC_REPORT as json
            top104_fwi = OrderedDict()
            top104_fwi['header'] = fwi_msg.header
            top104_fwi['body'] = fwi_msg.body

            # write json (top104_forecast) to output file
            flname = directory + "/" + 'FireWeatherIndex_' + str(i) + ".txt"
            with open(flname, 'w') as outfile:
                json.dump(top104_fwi, outfile, indent=4)

            print('Send ' + str(i) + ' message: Fire Weather Index has been forwarded to logger!')
            producer.send("TOP104_METRIC_REPORT", top104_fwi)
            counter_topics += 1

    # ----------------------------------------------------------------------------------
    # C) maxPFWI_Predicted Fire Weather Index --- 1st FWI per point
    for i in range(df_max.shape[0]):

        if df_max['Fire_Danger'].iloc[i] in categories[2:]:
            dataStreamGener = "CRCL"
            dataStreamName = "maxPFWI_maxPredicted Fire Weather Index"
            dataStreamID = 'FRCR_2003_maxPFWI'
            dataStreamDescript = "Canadian Fire Weather Index (FWI) per point of interst forwarding the max measurement which exceeds the Moderate Fire Danger category"
            lang = "en-US"
            dataStreamCategory = "Fire"
            dataStreamSubCategory = ""

            # Position (long/lat)
            LON_V = df_max['Long'].iloc[i]
            LAT_V = df_max['Lat'].iloc[i]
            position = [round(LON_V, 5), round(LAT_V, 5)]

            # Set variables for the header of the message
            district = "Valencia"

            # Unique message identifier
            msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")

            sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
            status = "Actual"
            actionType = "Update"
            scope = "Public"
            code = 20190617001

            # Call the class Top104_Metric_Report to create an object data of this class
            maxfwi_msg = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                              dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                              lang, dataStreamCategory, dataStreamSubCategory, position)

            # Record the thresholds for each weather indicator in the header note
            maxfwi_msg.topic_note = " "

            # create the header of the object
            maxfwi_msg.create_dictHeader()

            # create the measurements of the object
            #
            col_name = 'FWI' + "_" + interp_method
            maxfwi_msg.topic_yValue = [round(df_max.loc[i, col_name], 3)]

            maxfwi_msg.topic_measurementID = [int(round(time.time() * 1000))]

            # Measurement TimeStamp is equal with the Date/Time in which the predictions take place
            maxfwi_msg.topic_measurementTimeStamp = [str(df_max['Date'].iloc[0]) + 'Z']

            maxfwi_msg.topic_dataSeriesID = [str(int(abs(position[1]) * 100)) + str(int(abs(position[0]) * 100))]
            maxfwi_msg.topic_dataSeriesName = [df_max['Name'].iloc[i]]

            maxfwi_msg.topic_xValue = [str(df_max['Date'].iloc[i]) + 'Z']
            maxfwi_msg.topic_meas_color = [df_max['Color'].iloc[i]]
            maxfwi_msg.topic_meas_note = [df_max['Fire_Danger'].iloc[i]]

            # call class function
            maxfwi_msg.create_dictMeasurements()

            # create the body of the object
            maxfwi_msg.create_dictBody()

            # create the TOP104_METRIC_REPORT as json
            top104_maxfwi = OrderedDict()
            top104_maxfwi['header'] = maxfwi_msg.header
            top104_maxfwi['body'] = maxfwi_msg.body

            # write json (top104_forecast) to output file
            flname = directory + "/" + 'FireWeatherIndex_MAX_' + str(i) + ".txt"
            with open(flname, 'w') as outfile:
                json.dump(top104_maxfwi, outfile, indent=4)

            print('Send ' + str(i) + ' message: Max Fire Weather Index has been forwarded to logger!')
            producer.send("TOP104_METRIC_REPORT", top104_maxfwi)
            counter_topics += 1

    # ------------------------------------------------------------
    # A) Create TOP104 for PFRCL_Predicted Fire Crisis Level
    #
    dataStreamGener = "CRCL"
    dataStreamName = "PFRCL_Predicted Fire Crisis Level"
    dataStreamID = 'FRCR_2001_PCL'
    dataStreamDescript = "Overall Predicted Fire Crisis Level estimated per day over all the WFI value's of points"
    lang = "en-US"
    dataStreamCategory = "Fire"
    dataStreamSubCategory = ""

    #valid_values = ['Moderate', 'High', 'Very High', 'Extreme']
    valid_values = ['High', 'Very High', 'Extreme']

    #valid_values = df['Fire_Danger'].unique().split()[0]
    print("valid values= ", valid_values)

    for it, item in enumerate(FOCL_list, 1):

        if item['note'] in valid_values:
            # Position (long/lat)
            LAT_V = float(item['Position'][0])
            LON_V = float(item['Position'][1])
            position = [round(LON_V, 5), round(LAT_V, 5)]
            name_place = 'Parc Natural de lAlbufera'

            # Set variables for the header of the message
            district = "Valencia"

            # Unique message identifier
            msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")

            sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
            status = "Actual"
            actionType = "Update"
            scope = "Public"
            code = 20190617001

            # Call the class Top104_Metric_Report to create an object data of this class
            ovfcl_msg = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                             dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                             lang, dataStreamCategory, dataStreamSubCategory, position)

            # Record the thresholds for each weather indicator in the header note
            ovfcl_msg.topic_note = " "

            # create the header of the object
            ovfcl_msg.create_dictHeader()

            # create the measurements of the object
            #
            #ovfcl_msg.topic_yValue = [item['val']]
            ovfcl_msg.topic_yValue = [round(item['val_rescale'],2)]

            ovfcl_msg.topic_measurementID = [int(round(time.time() * 1000))]

            # Measurement TimeStamp is equal with the Date/Time in which the predictions take place
            ovfcl_msg.topic_measurementTimeStamp = [str(item['Date']) + 'Z']

            ovfcl_msg.topic_dataSeriesID = [str(int(abs(LAT_V) * 100)) + str(int(abs(LON_V) * 100))]
            ovfcl_msg.topic_dataSeriesName = [name_place]

            ovfcl_msg.topic_xValue = [str(item['Date']) + 'Z']
            ovfcl_msg.topic_meas_color = [item['color']]
            ovfcl_msg.topic_meas_note = [item['note']]

            # call class function
            ovfcl_msg.create_dictMeasurements()

            # create the body of the object
            ovfcl_msg.create_dictBody()

            # create the TOP104_METRIC_REPORT as json
            top104_fcl = OrderedDict()
            top104_fcl['header'] = ovfcl_msg.header
            top104_fcl['body'] = ovfcl_msg.body

            # write json (top104_forecast) to output file
            flname = directory + "/" + 'FireCrisisLevel_' + str(counter_topics) + ".txt"
            with open(flname, 'w') as outfile:
                json.dump(top104_fcl, outfile, indent=4)

            print('Send ' + str(counter_topics) + ' message: Overall Fire Weather Index has been forwarded to logger!')
            producer.send("TOP104_METRIC_REPORT", top104_fcl)
            counter_topics += 1

    print("Number of topics forwarded: ", counter_topics)
    return (counter_topics)
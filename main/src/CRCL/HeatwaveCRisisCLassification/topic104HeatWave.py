# Created Date: 08/06/2018
# Modified Date: 12/09/2018
#
#   Main function to create TOPICS 104 for the Heatwave pilot
#

from CRCL.HeatwaveCRisisCLassification.Topic104_Metric_Report import Top104_Metric_Report
from datetime import datetime, timedelta

from bus.bus_producer import BusProducer

from pathlib import Path
from collections import OrderedDict
import json, time, re


def topic104HeatWave(HOCL,First_HWCrisis_Event,Max_HWCrisis_Event,directory,points,center_points):

    # A) Create the TOPIC 104 (json format) for Heatwave Overall Crisis Level per day
    #       in the Thessaloniki region
    #
    counter_topics = 0

    producer = BusProducer()
    #
    #Decorate terminal
    print('\033[95m' + "\n***********************")
    print("*** CRCL SERVICE v1.0 ***")
    print("***********************\n" + '\033[0m')

    valid_values = ['Hot', 'Very Hot', 'Extreme']

    for it, item in enumerate(HOCL,1):

        if item['note'] in valid_values:

            dataStreamGener = "CRCL"
            dataStreamName = "PHWCL_Predicted HeatWave Crisis Level"
            dataStreamID = 'HWCR_3001_PCL'
            dataStreamDescript = "Heatwave Overall Crisis Level per day for the selected points. Date: " + str(item['DateTime'])
            lang = "en-US"
            dataStreamCategory = "Met"
            dataStreamSubCategory = "Heatwave"

            # Position (long/lat)
            position = [round(center_points[0], 5),
                        round(center_points[1], 5) ]

            # Set variables for the header of the message
            district = "Thessaloniki"

            # Unique message identifier
            msgIdent = datetime.utcnow().isoformat().replace(":","").replace("-","").replace(".","MS")

            sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
            status = "Actual"
            actionType = "Update"
            scope = "Public"
            code = 20190617001

            # Call the class Top104_Metric_Report to create an object data of this class
            hocl_msg = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                       dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                       lang, dataStreamCategory, dataStreamSubCategory, position)

            # Record the thresholds for each weather indicator in the header note
            hocl_msg.topic_note = " "

            # create the header of the object
            hocl_msg.create_dictHeader()

            # create the measurements of the object
            #
            hocl_msg.topic_yValue = [ item['val'] ]

            hocl_msg.topic_measurementID = [ int(round(time.time() * 1000)) ]
            hocl_msg.topic_measurementTimeStamp = [datetime.utcnow().replace(microsecond=0).isoformat() + 'Z']

            hocl_msg.topic_dataSeriesID = [str(int(position[1]*100)) + str(int(position[0]*100)) ]
            hocl_msg.topic_dataSeriesName = ["HOCL_dataSeries"]

            hocl_msg.topic_xValue = [ str( item['DateTime'] ) ]
            hocl_msg.topic_meas_color = [ item['color'] ]
            hocl_msg.topic_meas_note = [ item['note'] ]

            # call class function
            hocl_msg.create_dictMeasurements()

            # create the body of the object
            hocl_msg.create_dictBody()

            # create the TOP104_METRIC_REPORT as json
            top104_hocl = OrderedDict()
            top104_hocl['header'] = hocl_msg.header
            top104_hocl['body'] = hocl_msg.body

            # write json (top104_forecast) to output file
            flname = directory + "/" + 'TOP104_Heatwave_OverallCrisisLevel_' + str(it)  + ".txt"
            with open(flname, 'w') as outfile:
                json.dump(top104_hocl, outfile, indent=4)

            print('Send message: Heatwave Overall Crisis Level has been forwarded to logger!')
            producer.send("TOP104_METRIC_REPORT", top104_hocl)

            counter_topics += 1

    #----------------------------------------------------------------------------
    # B) Create TOPIC 104 for the 1st

    dataStreamGener = "CRCL"
    dataStreamName = "PDI_Predicted Discomfort Index"
    dataStreamID = 'HWCR_3002_PDI'
    dataStreamDescript = "Discomfort Index for Heatwave -- First DI value that overcomes the Most population feels discomfort category, per point"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Heatwave"

    # Set variables for the header of the message
    district = "Thessaloniki"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":","").replace("-","").replace(".","MS")

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    for fst in range(len(First_HWCrisis_Event)):

        # Position (lat/long)
        position = [ round(First_HWCrisis_Event[fst]['long'],5),
                     round(First_HWCrisis_Event[fst]['lat'], 5) ]

        name_place = First_HWCrisis_Event[fst]['Name']

        # Call the class Top104_Metric_Report to create an object data of this class
        fhwcr = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

        # Record the thresholds for each weather indicator in the header note
        fhwcr.topic_note = " "

        # create the header of the object
        fhwcr.create_dictHeader()

        # create the measurements of the object
        #
        fhwcr.topic_yValue = [ round(First_HWCrisis_Event[fst]['DI'], 3) ]

        fhwcr.topic_measurementID = [ int(round(time.time() * 1000)) ]
        fhwcr.topic_measurementTimeStamp = [datetime.utcnow().replace(microsecond=0).isoformat() + 'Z']

        dsid = str(int( float(position[1])*100 )) + str(int( float(position[0])*100 ))
        fhwcr.topic_dataSeriesID = [dsid]

        fhwcr.topic_dataSeriesName = ['First DI value in ' + name_place[0]]

        fhwcr.topic_xValue = [ str( First_HWCrisis_Event[fst]['DateTime'] ) ]
        fhwcr.topic_meas_color = [ First_HWCrisis_Event[fst]['Color'] ]
        fhwcr.topic_meas_note = [ First_HWCrisis_Event[fst]['DI_Category'] ]

        # call class function
        fhwcr.create_dictMeasurements()

        # create the body of the object
        fhwcr.create_dictBody()

        # create the TOP104_METRIC_REPORT as json
        top104_di = OrderedDict()
        top104_di['header']= fhwcr.header
        top104_di['body']= fhwcr.body

        # write json (top104_forecast) to output file
        flname = directory + "/" + 'TOP104_DiscomfortIndex_' + dsid + ".txt"
        with open(flname, 'w') as outfile:
            json.dump(top104_di, outfile, indent=4)

        print('Send message: Discomfort Index has been forwarded to logger!')
        producer.send("TOP104_METRIC_REPORT", top104_di)

        counter_topics += 1

    #----------------------------------------------------------------------------
    # C) Create TOPIC 104 for the Maximum

    dataStreamGener = "CRCL"
    dataStreamName = "maxPDI_Predicted Discomfort Index"
    dataStreamID = 'HWCR_3003_maxPDI'
    dataStreamDescript = "Discomfort Index for Heatwave -- Maximum DI value per point"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Heatwave"

    # Set variables for the header of the message
    district = "Thessaloniki"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":","").replace("-","").replace(".","MS")

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'
    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    for fst in range(len(Max_HWCrisis_Event)):

        # Position (lat/long)
        position = [ round(Max_HWCrisis_Event[fst]['long'],5),
                     round(Max_HWCrisis_Event[fst]['lat'], 5) ]

        name_place = Max_HWCrisis_Event[fst]['Name']

        # Call the class Top104_Metric_Report to create an object data of this class
        maxhwcr = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

        # Record the thresholds for each weather indicator in the header note
        maxhwcr.topic_note = " "

        # create the header of the object
        maxhwcr.create_dictHeader()

        # create the measurements of the object
        #
        maxhwcr.topic_yValue = [ round(Max_HWCrisis_Event[fst]['DI'], 3) ]

        maxhwcr.topic_measurementID = [ int(round(time.time() * 1000)) ]
        maxhwcr.topic_measurementTimeStamp = [datetime.utcnow().replace(microsecond=0).isoformat() + 'Z']

        dsid = str(int( float(position[1])*100 )) + str(int( float(position[0])*100 ))
        maxhwcr.topic_dataSeriesID = [dsid]

        maxhwcr.topic_dataSeriesName = ['Max DI value in ' + name_place[0] ]

        maxhwcr.topic_xValue = [ str( Max_HWCrisis_Event[fst]['DateTime'] ) ]
        maxhwcr.topic_meas_color = [ Max_HWCrisis_Event[fst]['Color'] ]
        maxhwcr.topic_meas_note = [ Max_HWCrisis_Event[fst]['DI_Category'] ]

        # call class function
        maxhwcr.create_dictMeasurements()

        # create the body of the object
        maxhwcr.create_dictBody()

        # create the TOP104_METRIC_REPORT as json
        top104_mxdi = OrderedDict()
        top104_mxdi['header']= maxhwcr.header
        top104_mxdi['body']= maxhwcr.body

        # write json (top104_forecast) to output file
        flname = directory + "/" + 'TOP104_Max_DiscomfortIndex_' + dsid + ".txt"
        with open(flname, 'w') as outfile:
            json.dump(top104_mxdi, outfile, indent=4)

        print('Send message: Max Discomfort Index has been forwarded to logger!')
        producer.send("TOP104_METRIC_REPORT", top104_mxdi)

        counter_topics += 1

    return(counter_topics)


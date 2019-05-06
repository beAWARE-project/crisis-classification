# Created Date: 10/10/2018
# Modified Date:
#
#   Main function to create TOPICS 104 for the Heatwave pilot (2nd Development Phase)
#

from CRCL.HeatwaveCRisisCLassification.Top104_Metric_Report import Top104_Metric_Report
from datetime import datetime, timedelta

from bus.bus_producer import BusProducer

from pathlib import Path
from collections import OrderedDict
import json, time, re

def topic104HeatWave(HOCL, First_HWCrisis_Event, Max_HWCrisis_Event, First_RemainingHours, Max_RemainingHours,
                     directory, points, center_points, valid_values):

    # Difference between UTC and local time
    offset = datetime.now() - datetime.utcnow()

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

    for it, item in enumerate(HOCL,1):

        if item['note'] in valid_values:

            dataStreamGener = "CRCL"
            dataStreamName = "PHWCL_Predicted HeatWave Crisis Level"
            dataStreamID = 'HWCR_3001_PCL' + '_Day_'+ str(it)
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

            # Version 9 --- Send Local Time
            # msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")
            # sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'

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

            # Ver 9
            # hocl_msg.topic_measurementTimeStamp = [datetime.now().replace(microsecond=0).isoformat() + 'Z']

            hocl_msg.topic_dataSeriesID = [str(int(position[1]*100)) + str(int(position[0]*100)) ]
            hocl_msg.topic_dataSeriesName = ["HOCL_dataSeries"]

            hocl_msg.topic_xValue = [ str( item['DateTime'] ) ]

            # Ver 9 -- Convert UTC To Local Time
            # uct_dt = datetime.strptime(item['DateTime'], "%Y-%m-%dT%H:%M:%S")
            # current_dt = uct_dt + offset
            # hocl_msg.topic_xValue = [ str( current_dt ) ]

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

    # Version 9 --- Send Local Time
    # msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")
    # sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'

    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    for fst in range(len(First_HWCrisis_Event)):

        # Position (lat/long)
        position = [ round(float(First_HWCrisis_Event[fst]['long']),5),
                     round(float(First_HWCrisis_Event[fst]['lat']), 5) ]

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

        # Ver 9 -- Send Local Time
        # fhwcr.topic_measurementTimeStamp = [datetime.now().replace(microsecond=0).isoformat() + 'Z']

        dsid = str(int( float(position[1])*100 )) + str(int( float(position[0])*100 ))
        fhwcr.topic_dataSeriesID = [dsid]

        fhwcr.topic_dataSeriesName = ['First DI value in ' + name_place]

        fhwcr.topic_xValue = [ str( First_HWCrisis_Event[fst]['DateTime'] ) ]

        # Ver 9 -- Convert UTC To Local Time
        # uct_frsdt = datetime.strptime( First_HWCrisis_Event[fst]['DateTime'], "%Y-%m-%dT%H:%M:%S")
        # current_frsdt = uct_frsdt + offset
        # fhwcr.topic_xValue = [str(current_frsdt)]

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
        flname = directory + "/" + 'TOP104_DiscomfortIndex' + '_' + dsid + '_' + name_place + ".txt"
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

    # Version 9 --- Send Local Time
    # msgIdent = datetime.now().isoformat().replace(":", "").replace("-", "").replace(".", "MS")
    # sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'

    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    for fst in range(len(Max_HWCrisis_Event)):

        # Position (lat/long)
        position = [ round(float(Max_HWCrisis_Event[fst]['long']),5),
                     round(float(Max_HWCrisis_Event[fst]['lat']), 5) ]

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

        # Ver 9 -- Send Local Time
        # maxhwcr.topic_measurementTimeStamp = [datetime.now().replace(microsecond=0).isoformat() + 'Z']

        dsid = str(int( float(position[1])*100 )) + str(int( float(position[0])*100 ))
        maxhwcr.topic_dataSeriesID = [dsid]

        maxhwcr.topic_dataSeriesName = ['Max DI value in ' + name_place]

        maxhwcr.topic_xValue = [ str( Max_HWCrisis_Event[fst]['DateTime'] ) ]

        # Ver 9 -- Convert UTC To Local Time
        # uct_maxdt = datetime.strptime( Max_HWCrisis_Event[fst]['DateTime'], "%Y-%m-%dT%H:%M:%S")
        # current_maxdt = uct_maxdt + offset
        # maxhwcr.topic_xValue = [str(current_maxdt)]


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
        flname = directory + "/" + 'TOP104_Max_DiscomfortIndex' + '_' + dsid + '_' + name_place + ".txt"
        with open(flname, 'w') as outfile:
            json.dump(top104_mxdi, outfile, indent=4)

        print('Send message: Max Discomfort Index has been forwarded to logger!')
        producer.send("TOP104_METRIC_REPORT", top104_mxdi)

        counter_topics += 1

    #----------------------------------------------------------------------------
    # D) Create TOPIC 104 for the First Remaining Hours

    dataStreamGener = "CRCL"
    dataStreamName = "RH_Remaining Hours To Heatwave"
    dataStreamID = 'HWCR_3102_RH'
    dataStreamDescript = "Remaining Hours To Heatwave based on the first value of the Discomfort Index for a Heatwave incident per location"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Heatwave"

    # Set variables for the header of the message
    district = "Thessaloniki"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":","").replace("-","").replace(".","MS")

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'

    # Ver 9 --- send Local Time
    # msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")
    # sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'

    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    for fst in range(len(First_RemainingHours)):

        # Position (lat/long)
        position = [ round(float(First_RemainingHours[fst]['long']),5),
                     round(float(First_RemainingHours[fst]['lat']), 5) ]

        name_place = First_RemainingHours[fst]['Name']

        # Call the class Top104_Metric_Report to create an object data of this class
        fhwrh = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

        # Record the thresholds for each weather indicator in the header note
        fhwrh.topic_note = " "

        # create the header of the object
        fhwrh.create_dictHeader()

        # create the measurements of the object
        #
        fhwrh.topic_yValue = [ round(First_RemainingHours[fst]['Remaining Hours'], 3) ]

        fhwrh.topic_measurementID = [ int(round(time.time() * 1000)) ]
        fhwrh.topic_measurementTimeStamp = [datetime.utcnow().replace(microsecond=0).isoformat() + 'Z']

        # Ver 9 --- Send Local Time
        # fhwrh.topic_measurementTimeStamp = [datetime.now().replace(microsecond=0).isoformat() + 'Z']

        dsid = str(int( float(position[1])*100 )) + str(int( float(position[0])*100 ))
        fhwrh.topic_dataSeriesID = [dsid]

        fhwrh.topic_dataSeriesName = ['Remaining Hours to Heatwave in ' + name_place]

        fhwrh.topic_xValue = [ str( First_RemainingHours[fst]['DateTime'] ) ]

        # Ver 9 -- Convert UTC To Local Time
        # uct_frhdt = datetime.strptime( First_RemainingHours[fst]['DateTime'], "%Y-%m-%dT%H:%M:%S")
        # current_frhdt = uct_frhdt + offset
        # fhwrh.topic_xValue = [str(current_frhdt)]


        fhwrh.topic_meas_color = [ None ]
        fhwrh.topic_meas_note = [ None ]

        # call class function
        fhwrh.create_dictMeasurements()

        # create the body of the object
        fhwrh.create_dictBody()

        # create the TOP104_METRIC_REPORT as json
        top104_di = OrderedDict()
        top104_di['header']= fhwrh.header
        top104_di['body']= fhwrh.body

        # write json (top104_forecast) to output file
        flname = directory + "/" + 'TOP104_RemainingHours_First_DI' + '_' + dsid + '_' + name_place + ".txt"
        with open(flname, 'w') as outfile:
            json.dump(top104_di, outfile, indent=4)

        print('Send message: Remaining Hours for the 1st hazardous event of the Discomfort Index has been forwarded to logger!')
        producer.send("TOP104_METRIC_REPORT", top104_di)

        counter_topics += 1

    #----------------------------------------------------------------------------
    # E) Create TOPIC 104 for the Max Remaining Hours

    dataStreamGener = "CRCL"
    dataStreamName = "maxRH_Remaining Hours To Heatwave"
    dataStreamID = 'HWCR_3103_maxRH'
    dataStreamDescript = "Maximum Remaining Hours To Heatwave based on the maximum value of the Discomfort Index for a Heatwave incident per location"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Heatwave"

    # Set variables for the header of the message
    district = "Thessaloniki"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":","").replace("-","").replace(".","MS")

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'

    # Ver 9 -- send local time
    # msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")
    # sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'

    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    for fst in range(len(Max_RemainingHours)):

        # Position (lat/long)
        position = [ round(float(Max_RemainingHours[fst]['long']),5),
                     round(float(Max_RemainingHours[fst]['lat']), 5) ]

        name_place = Max_RemainingHours[fst]['Name']

        # Call the class Top104_Metric_Report to create an object data of this class
        fhwmaxrh = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

        # Record the thresholds for each weather indicator in the header note
        fhwmaxrh.topic_note = " "

        # create the header of the object
        fhwmaxrh.create_dictHeader()

        # create the measurements of the object
        #
        fhwmaxrh.topic_yValue = [ round(Max_RemainingHours[fst]['Remaining Hours'], 3) ]

        fhwmaxrh.topic_measurementID = [ int(round(time.time() * 1000)) ]
        fhwmaxrh.topic_measurementTimeStamp = [datetime.utcnow().replace(microsecond=0).isoformat() + 'Z']

        # Ver 9 -- send local time
        # fhwmaxrh.topic_measurementTimeStamp = [datetime.now().replace(microsecond=0).isoformat() + 'Z']

        dsid = str(int( float(position[1])*100 )) + str(int( float(position[0])*100 ))
        fhwmaxrh.topic_dataSeriesID = [dsid]

        fhwmaxrh.topic_dataSeriesName = ['Remaining Hours (Max) to Heatwave in ' + name_place]

        fhwmaxrh.topic_xValue = [ str( Max_RemainingHours[fst]['DateTime'] ) ]

        # Ver 9 --- Convert UTC time to Local time
        # uct_maxrhdt = datetime.strptime( Max_RemainingHours[fst]['DateTime'], "%Y-%m-%dT%H:%M:%S")
        # current_maxrhdt = uct_maxrhdt + offset
        # fhwmaxrh.topic_xValue = [str(current_maxrhdt)]


        fhwmaxrh.topic_meas_color = [ None ]
        fhwmaxrh.topic_meas_note = [ None ]

        # call class function
        fhwmaxrh.create_dictMeasurements()

        # create the body of the object
        fhwmaxrh.create_dictBody()

        # create the TOP104_METRIC_REPORT as json
        top104_di = OrderedDict()
        top104_di['header']= fhwmaxrh.header
        top104_di['body']= fhwmaxrh.body

        # write json (top104_forecast) to output file
        flname = directory + "/" + 'TOP104_RemainingHours_Max_DI' + '_' + dsid + '_' + name_place + ".txt"
        with open(flname, 'w') as outfile:
            json.dump(top104_di, outfile, indent=4)

        print('Send message: Maximum Remaining Hours for the maximum value of Discomfort Index has been forwarded to logger!')
        producer.send("TOP104_METRIC_REPORT", top104_di)

        counter_topics += 1


    return(counter_topics)


#==================================================================================================
def topic104HeatWave_Emerg(HOCL_to_topic, HWCrisis_Event, directory, points, center_points, valid_values):

    # Difference between UTC and local time
    offset = datetime.now() - datetime.utcnow()

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

    for it, item in enumerate(HOCL_to_topic, 1):

        if item['note'] in valid_values:
            dataStreamGener = "CRCL"
            dataStreamName = "OHWCL_Observed HeatWave Crisis Level"
            dataStreamID = 'HWCR_3011_OCL'
            dataStreamDescript = "Observed Heatwave Overall Crisis Level per day for the selected points. Date: " + str(item['DateTime'])
            lang = "en-US"
            dataStreamCategory = "Met"
            dataStreamSubCategory = "Heatwave"

            # Position (long/lat)
            position = [round(center_points[0], 5),
                        round(center_points[1], 5)]

            # Set variables for the header of the message
            district = "Thessaloniki"

            # Unique message identifier
            msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")

            sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'

            # Ver 3 -- send local time
            # msgIdent = datetime.now().isoformat().replace(":", "").replace("-", "").replace(".", "MS")
            # sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'

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
            hocl_msg.topic_yValue = [item['val']]

            hocl_msg.topic_measurementID = [int(round(time.time() * 1000))]
            hocl_msg.topic_measurementTimeStamp = [datetime.utcnow().replace(microsecond=0).isoformat() + 'Z']

            # Ver 3 -- send local time
            # hocl_msg.topic_measurementTimeStamp = [datetime.now().replace(microsecond=0).isoformat() + 'Z']

            hocl_msg.topic_dataSeriesID = [str(int(position[1] * 100)) + str(int(position[0] * 100))]
            hocl_msg.topic_dataSeriesName = ["HOCL_dataSeries"]

            hocl_msg.topic_xValue = [str(item['DateTime'])]

            # Ver 3 Convert UTC to Local Time
            # uct_dt = datetime.strptime( item['DateTime'], "%Y-%m-%dT%H:%M:%S")
            # current_dt = uct_dt + offset
            # hocl_msg.topic_xValue = [str(current_dt)]


            hocl_msg.topic_meas_color = [item['color']]
            hocl_msg.topic_meas_note = [item['note']]

            # call class function
            hocl_msg.create_dictMeasurements()

            # create the body of the object
            hocl_msg.create_dictBody()

            # create the TOP104_METRIC_REPORT as json
            top104_hocl = OrderedDict()
            top104_hocl['header'] = hocl_msg.header
            top104_hocl['body'] = hocl_msg.body

            # write json (top104_forecast) to output file
            flname = directory + "/" + 'TOP104_Heatwave_OverallCrisisLevel_' + str(it) + ".txt"
            with open(flname, 'w') as outfile:
                json.dump(top104_hocl, outfile, indent=4)

            print('Send message: Observed Heatwave Overall Crisis Level has been forwarded to logger!')
            producer.send("TOP104_METRIC_REPORT", top104_hocl)

            counter_topics += 1

    #----------------------------------------------------------------------------
    # B) Create TOPIC 104 for each area where it exceeds a predefined category

    dataStreamGener = "CRCL"
    dataStreamName = "ODI_Observed Discomfort Index"
    dataStreamID = 'HWCR_3012_ODI'
    dataStreamDescript = "Discomfort Index for Heatwave -- Observed DI value that overcomes the Most population feels discomfort category, per point"
    lang = "en-US"
    dataStreamCategory = "Met"
    dataStreamSubCategory = "Heatwave"

    # Set variables for the header of the message
    district = "Thessaloniki"

    # Unique message identifier
    msgIdent = datetime.utcnow().isoformat().replace(":","").replace("-","").replace(".","MS")

    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'

    # Ver 3 --- send local time
    # msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")
    # sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'

    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = 20190617001

    for fst in range(len(HWCrisis_Event)):

        # Position (lat/long)
        position = [ round(float(HWCrisis_Event[fst]['long']),5),
                     round(float(HWCrisis_Event[fst]['lat']), 5) ]

        name_place = HWCrisis_Event[fst]['Name']

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
        fhwcr.topic_yValue = [ round(HWCrisis_Event[fst]['DI'], 3) ]

        fhwcr.topic_measurementID = [ int(round(time.time() * 1000)) ]
        fhwcr.topic_measurementTimeStamp = [datetime.utcnow().replace(microsecond=0).isoformat() + 'Z']

        # Ver 3 -- send local time
        # fhwcr.topic_measurementTimeStamp = [datetime.now().replace(microsecond=0).isoformat() + 'Z']

        dsid = str(int( float(position[1])*100 )) + str(int( float(position[0])*100 ))
        fhwcr.topic_dataSeriesID = [dsid]

        fhwcr.topic_dataSeriesName = ['Observed DI value in ' + name_place]

        fhwcr.topic_xValue = [ str( HWCrisis_Event[fst]['DateTime'] ) ]

        # Ver 3 -- Convert UTC time to local time
        # uct_hwdt = datetime.strptime( HWCrisis_Event[fst]['DateTime'], "%Y-%m-%dT%H:%M:%S")
        # current_hwdt = uct_hwdt + offset
        # fhwcr.topic_xValue = [ str( current_hwdt ) ]

        fhwcr.topic_meas_color = [ HWCrisis_Event[fst]['Color'] ]
        fhwcr.topic_meas_note = [ HWCrisis_Event[fst]['DI_Category'] ]

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

    return(counter_topics)
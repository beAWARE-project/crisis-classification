# Created Date: 07/03/2018
# Modified Date: 15/03/2018
#
# Implements the 1st algorithm of Crisis Classification module
# based on the predicted water levels from AMICO for particular 60
# river sections in the next 54h starting at a specific date/time or
# the last execution of AMICO module.
#
# CRCL_from_Forecast calculates the scale (0-3) for each river section and the
#   Overall Crisis Classification Index for each river's group of sections and
#   whole Vicenza city.
#
#----------------------------------------------------------------------------------------------------------
# Inputs: a) Time series of predicted water levels from AMICO for each one of the
#            interest river section in the next 54h starting a specific date/time or
#            for the lastRun of AMICO's program
#         b) Thresholds for each one of the river section
#
# Outputs: TOP104_METRIC_REPORT which contains the maximum predicted crisis level in the next 54h for
#           the particular river section (pre-alert visualization)
#
#   Algorithm 1 from Crisis Classification (based on AAWA)
#----------------------------------------------------------------------------------------------------------
#

from bus.bus_producer import BusProducer
from bus.CRCL_service import CRCLService
import json, time, re
import os, errno
from pathlib import Path
from pandas import read_csv
from datetime import datetime, timedelta
from math import pow, ceil

from Top104_Metric_Report import Top104_Metric_Report
from Create_Queries import extract_forecasts
from Create_Queries import extract_river_sections_loc
from Auxiliary_functions import compare_forecast_scale_thresholds, generalized_mean, Overall_Crisis_Classification_Index


# Create a directory to store the output files and TOPICS
root_path = Path.cwd()
#directory = "TOPICS_Ver6_testdates"
directory = "TOPICS_Ver6"
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

#------------------------------------------------------------------------------------------------
# STEP 1: Extract the ids, the names, the properties and the location of all river sections
#
# https://beaware.server.de/SensorThingsService/v1.0/Things
# ? $filter=properties/type%20eq%20%27riverSection%27
# & $select=id,name,properties
# & $expand=Locations($select=description,location)
# & $count=true
# & $top=1000

# Start Timing Step 1
start_step1 = time.time()

SensorThings = [SensorThingEntities[0], SensorThingEntities[1]]

filt_vals = 'riverSection'
sel_vals = {'thing_sel': ['id', 'name', 'description','properties'],
            'loc_sel': ['description', 'location']}

riverSections = extract_river_sections_loc(service_root_URI, SensorThings, filt_vals, sel_vals)

# write json (data) to output file
flname = directory + "/" + 'response_riverSections.txt'
with open(flname, 'w') as outfile:
    json.dump(riverSections, outfile)

# count: number of river sections to be examined. Total river sections is 304.
count = riverSections["@iot.count"]

# End Timing Step 1
end_step1 = time.time()
time_duration_step1 = end_step1 - start_step1

#----------------------------------------------------------------------------------------------------
# STEP 2: Extract predicted water levels from AMICO for each one of the interest river sections
#           in the next 54h and find the maximum value, compares it with predefined thresholds.
#           If this max value exceeds the thresholds and a new scale (metric) with values 0 to 3
#           is calculated based on the result of comparison. The Overall Crisis Classification Index
#           is calculated per group of river sections and whole region. The appropriate messages
#           are created and sent them to logger.
#----------------------------------------------------------------------------------------------------
# 2.1 Extract one measurement (forecast for water river level) from one station at specific date/time
#
# ex. Things id 390 -> River section Astico m .00
#     Date -> 2018-01-26T08:00:00.000Z

# Start Timing Step 2
start_step2 = time.time()

# Import interesting river sections from file (csv)
IntRS = read_csv('Amico_RS_in_Vicenza_v4.csv', sep=",")

# Set constant variables which are utilised to create the query to extract Observations of each River Section
#
SensorThings = [SensorThingEntities[0], SensorThingEntities[1], SensorThingEntities[3], SensorThingEntities[5]]
sel_vals = {'dstr_sel': ['id', 'name', 'properties'], 'obs_sel': ['result', 'phenomenonTime', 'id', 'parameters']}
filt_args={'obs_filt': ['phenomenonTime']}
dates = ['2018-01-26T08:00:00.000Z', '2018-01-28T14:00:00.000Z']
#dates = ['2016-01-26T08:00:00.000Z', '2016-01-28T14:00:00.000Z']
filt_vals={'obs_filt_vals': dates}
ord_vals = ['phenomenonTime']

flag_last_run = True #False

#----------------------------------------------------------------------------------------
# Create new Producer instance using provided configuration message (dict data).

producer = BusProducer()

# Decorate terminal
print('\033[95m' + "\n***********************")
print("*** CRCL SERVICE v1.0 ***")
print("***********************\n" + '\033[0m')

total_irs_names = 0
total_top104 = 0

# Initialize the list of dictionaries. Each one contains the name of the group of River Sections
# and a vector of the scale cardinality (count = [n0, n1, n2, n3] for each group)
RiverSect_CountScale = [{'name': 'ASTICO', 'count': [0,0,0,0]},
                        {'name': 'LEOGRA', 'count': [0,0,0,0]},
                        {'name': 'OROLO',  'count': [0,0,0,0]},
                        {'name': 'PIOVEGO', 'count': [0,0,0,0]},
                        {'name': 'RETRONE', 'count': [0,0,0,0]},
                        {'name': 'TIMONCHIO', 'count': [0,0,0,0]},
                        {'name': 'BACCHIGLIONE', 'count': [0,0,0,0]}]

for counter in range(0, count):

    if len( IntRS[ IntRS.ix[:,'Name'].str.contains(riverSections["value"][counter]['name']) ] ) != 0:

        print("\n River Section Name: ", riverSections["value"][counter]['name'], ", ID: ", riverSections["value"][counter]['@iot.id'])
        total_irs_names = total_irs_names + 1

        # find the position of RiverSect_CountScale in which the river section name matches
        # with the name of group of river sections
        nm1 = riverSections["value"][counter]['name'].upper()

        pos_RSCS = -1
        for i in range(0, len(RiverSect_CountScale)):
            if RiverSect_CountScale[i]['name'] in nm1 :
                pos_RSCS = i

        if pos_RSCS == -1:
            print(" ERROR: river section name does not match with the name of Group River Sections!!! ")

        # Arrays to store values from the TOP104 (initialize for each river section)
        max_yValues = []
        meas_color = []
        meas_note = []
        max_measurementID = []
        max_measurementTimeStamp = []
        dataSeriesID = []
        dataSeriesName = []

        ids = {'th_id': str(riverSections["value"][counter]['@iot.id']) }

        if flag_last_run == False:
            response_forecast = extract_forecasts(service_root_URI, SensorThings, ids, sel_vals, ord_vals, filter_args=filt_args, filter_vals=filt_vals)
        else:
            response_forecast = extract_forecasts(service_root_URI, SensorThings, ids, sel_vals, ord_vals, last_run=flag_last_run)

        # write json (data) to output file
        # flname = directory + "/" + 'response_forecast_' + riverSections["value"][counter]['name'].replace(" ", "") + ".txt"
        # with open(flname, 'w') as outfile:
        #    json.dump(response_forecast, outfile)

        # Extract the thresholds of the response of riverSections query correspond to the specific river section
        #thresh = [riverSections["value"][counter]['properties']['treshold1'],
        #          riverSections["value"][counter]['properties']['treshold2'],
        #          riverSections["value"][counter]['properties']['treshold3']]

        loc_riverSection = riverSections["value"][counter]['Locations'][0]['location']['coordinates']

        # ΘΑ ΠΡΕΠΕΙ ΝΑ ΑΛΛΑΞΕΙ ΣΕ ΠΡΑΓΜΑΤΙΚΟ ΤΡΕΞΙΜΟ. ΝΑ ΔΙΑΓΡΑΦΕΙ ΧΡΗΣΗ ΠΡΑΓΜΑΤΙΚΩΝ ΚΑΤΩΦΛΙΩΝ
        thresh = [20, 25, 30]
        #thresh = [30, 35, 40]

        # Extract the observations WL forecasted values and stored in the array yValues
        Obs_yV_length = len(response_forecast['Datastreams'][0]['Observations'])

        Obs_yv = []
        for iter in range(0, Obs_yV_length):
            Obs_yv += [response_forecast['Datastreams'][0]['Observations'][iter]['result']]

        # Find all the maximum of the Obs_yv and its positions
        Obs_yv_max = max(Obs_yv)
        maxIndexList = [i for i,j in enumerate(Obs_yv) if j == Obs_yv_max]
        first_max_pos = [min(maxIndexList)]  # considers only the first maximum value

        # Calculate the Crisis Classification Level for each River Section
        #   If the maximum value exceeds one of the predefined thresholds then
        #   it stored in the topic (flag_extreme=True), otherwise it ignores (flag_extreme = False)
        #
        resp_comparison = compare_forecast_scale_thresholds(Obs_yv_max, thresh)

        flag_extreme = resp_comparison[len(resp_comparison) - 1]

        print("**** resp_comparison = ", resp_comparison)

        if flag_extreme == True and resp_comparison[0][0] != '#00FF00':
            max_yValues += [Obs_yv_max]          # for forecast
            max_yValues += [resp_comparison[2]]  # for scale

            # update the count in position equal with the scale adding one
            # for the particular group river sections defined by pos_RSCS
            RiverSect_CountScale[pos_RSCS]['count'][ max_yValues[1] ] += 1

            meas_color.append( resp_comparison[0][0] )   # for forecast
            meas_color.append("")                        # for scale

            meas_note.append( resp_comparison[1][0] )    # for forecast
            meas_note.append( resp_comparison[3][0] )    # for scale

            dataSeriesID += [riverSections["value"][counter]['@iot.id']]*len(max_yValues)  # counter + 1
            dataSeriesName += [riverSections["value"][counter]['name']]*len(max_yValues)

            # Find details regarding the maximum observation and stored them in the corresponding arrays
            item = response_forecast['Datastreams'][0]['Observations'][first_max_pos[0]]
            max_measurementID += [ str(item['@iot.id']) + '_1' ] # for forecast
            max_measurementID += [ str(item['@iot.id']) + '_2' ] # for scale
            max_measurementTimeStamp += [item['phenomenonTime'].replace('.000Z', "Z")]*len(max_yValues)

            #--------------------------------------------------------------------------------------------
            #  STEP 2.2: Creates the TOPIC_104_METRIC_REPORT
            #--------------------------------------------------------------------------------------------
            #
            # Create the TOPIC 104 (json format) for the maximum value of predicted water levels
            # in the time interval defined by the 'dates' or for the lastRun of AMICO's program
            # for the specific river section.
            #
            # Set variables for the body of the message

            dataStreamGener = "CRCL"
            dataStreamName = "River Water Level Forecast"

            if flag_last_run == True:
                lastRunID = response_forecast['Datastreams'][0]["properties"]["lastRunId"]
                dataStreamID = str(lastRunID) + "_" + str(datetime.now().microsecond)
                dataStreamDescript = "AMICO predictions of water level in the last run with ID:" + str(lastRunID)
            else:
                ObsRunID = response_forecast['Datastreams'][0]['Observations'][0]["parameters"]["runId"]
                dataStreamID = ObsRunID
                dataStreamDescript = "AMICO predictions of water level in the run with ID:" + str(ObsRunID) + " at dates: " + str(dates[0]) + " to " + str(dates[1])
            lang = "it-IT"
            dataStreamCategory = "Met"
            dataStreamSubCategory = "Flood"

            # Position of the specific river section
            position = loc_riverSection

            # Set variables for the header of the message
            district = "Vicenza"

            # Unique message identifier
            msgIdent = datetime.now().isoformat().replace(":","").replace("-","").replace(".","MS")

            sent_dateTime = datetime.now().replace(microsecond=0).isoformat() + 'Z'
            status = "Actual"
            actionType = "Update"
            scope = "Public"
            code = 20190617001

            # Call the class Top104_Metric_Report to create an object data of this class
            #
            data = Top104_Metric_Report(msgIdent, sent_dateTime, status, actionType, scope, district, code,
                                    dataStreamGener, dataStreamID, dataStreamName, dataStreamDescript,
                                    lang, dataStreamCategory, dataStreamSubCategory, position)

            # Record the thresholds for each river Section in the header note
            data.topic_note = "Threshold_1=" + str(thresh[0]) + ", " + "Threshold_2=" + str(thresh[1]) + ", " + "Threshold_3=" + str(thresh[2])

            # create the header of the object
            data.create_dictHeader()

            # create the measurements of the object
            #
            data.topic_yValue = max_yValues
            data.topic_measurementID = max_measurementID
            data.topic_measurementTimeStamp = max_measurementTimeStamp
            data.topic_dataSeriesID = dataSeriesID
            data.topic_dataSeriesName = dataSeriesName
            data.topic_xValue = [""]*len(max_yValues)
            data.topic_meas_color = meas_color
            data.topic_meas_note = meas_note

            # call class function
            data.create_dictMeasurements()

            # create the body of the object
            data.create_dictBody()

            # create the TOP104_METRIC_REPORT as json
            top104_forecast = {'header': data.header, 'body': data.body}

            # write json (top104_forecast) to output file
            flname = directory + "/" + 'TOP104_forecasts_' + riverSections["value"][counter]['name'].replace(" ", "") + ".txt"
            with open(flname, 'w') as outfile:
                json.dump(top104_forecast, outfile, indent=4)

            print('Send message: Max Predicted Water Level value has been forwarded to logger!')

            producer.send("TOP104_METRIC_REPORT", top104_forecast)
            total_top104 = total_top104 + 1

# End Timing Step 2
end_step2 = time.time()
time_duration_step2 = end_step2 - start_step2

#---------------------------------------------------------------------
# STEP 3: Calculate the Overall Crisis Classification Index

print("  RiverSect_CountScale = ", RiverSect_CountScale)

# Start Timing Step 3
start_step3 = time.time()

over_crisis_class_indx = Overall_Crisis_Classification_Index( RiverSect_CountScale )

# Creates TOP104 for the Overall_Crisis_Classification_Index per group river sections and total
# Set variables for the body of the message

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
len_occi = len(over_crisis_class_indx)

occi_msg.topic_yValue = [over_crisis_class_indx[len_occi - 1]['occi']]
occi_msg.topic_measurementID = ['Meas_ID_1']
occi_msg.topic_measurementTimeStamp = [sent_dateTime]
occi_msg.topic_dataSeriesID = ['DataS_ID_1']
occi_msg.topic_dataSeriesName = [over_crisis_class_indx[len_occi - 1]['name']]
occi_msg.topic_xValue = [""]
occi_msg.topic_meas_color = [over_crisis_class_indx[len_occi - 1]['color']]
occi_msg.topic_meas_note = [over_crisis_class_indx[len_occi - 1]['note']]

# call class function
occi_msg.create_dictMeasurements()

# create the body of the object
occi_msg.create_dictBody()

# create the TOP104_METRIC_REPORT as json
top104_occi = {'header': occi_msg.header, 'body': occi_msg.body}

# write json (top104_occi) to output file
flname = directory + "/" + "TOP104_OverallCrisiClassIndx.txt"
with open(flname, 'w') as outfile:
    json.dump(top104_occi, outfile, indent=4)

print('Send message: Overall Crisis Classification Index has been forwarded to logger!')
producer.send("TOP104_METRIC_REPORT", top104_occi)

total_top104 = total_top104 + 1


# End Timing Step 3
end_step3 = time.time()
time_duration_step3 = end_step3 - start_step3


#---------------------------------------------------------------------------
total_time = time_duration_step1 + time_duration_step2 + time_duration_step3

print("\n ****** EXECUTION TIME: **** ")
print(" Time for Step 1: ", time_duration_step1, " seconds")
print(" Time for Step 2: ", time_duration_step2, " seconds")
print(" Time for Step 3: ", time_duration_step3, " seconds")
print(" Total Execution Time: ", total_time/60.0, " minutes")

print(" Total interested River Sections = ", total_irs_names)
print(" Number of TOP104 which were sent to PSAP is: ", total_top104)
print(" ************************** \n")


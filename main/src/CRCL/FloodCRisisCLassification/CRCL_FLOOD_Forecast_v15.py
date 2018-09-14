# Created Date: 11/09/2018
# Modified Date: 12/09/2018
#
# Implements the 1st algorithm of Crisis Classification module
# based on the predicted water levels from AMICO for particular 60
# river sections in the next 54h starting at a specific date/time or
# the last execution of AMICO module.
#
# Groups of River Sections are inserted by a csv/xlsx file
#
# CRCL_from_Forecast calculates the scale (1-4) for each river section and the
#   Overall Crisis Level index for each river's group of sections and
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
import json, time, re
import os, errno
from pathlib import Path
from pandas import read_csv, DataFrame, concat, ExcelWriter
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from math import pow, ceil
from collections import OrderedDict

from CRCL.FloodCRisisCLassification.Topic104_Metric_Report import Top104_Metric_Report
from CRCL.FloodCRisisCLassification.Create_Queries import extract_forecasts, extract_river_sections_loc
from CRCL.FloodCRisisCLassification.Auxiliary_functions import *
from CRCL.FloodCRisisCLassification.topic104Flood import *

def CrisisClassificationFlood_PreEmerg():

    ver = 'Ver15_2nd_Period'

    # Create a directory to store the output files and TOPICS
    # Create a path
    current_dirs_parent = os.getcwd()
    root_path_dir = current_dirs_parent + "/" + "CRCL/FloodCRisisCLassification" + "/"

    now = datetime.now()

    directory = root_path_dir + "TOPICS_for_2010" + "_" + ver + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day)
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

    # Store the time steps
    time_duration_step = []

    SensorThings = [SensorThingEntities[0], SensorThingEntities[1]]

    filt_vals = 'riverSection'
    sel_vals = {'thing_sel': ['id', 'name', 'description','properties'],
                'loc_sel': ['description', 'location']}

    riverSections = extract_river_sections_loc(service_root_URI, SensorThings, filt_vals, sel_vals)

    # write json (data) to output file
    flname = directory + "/" + 'response_riverSections.txt'
    with open(flname, 'w') as outfile:
        json.dump(OrderedDict(riverSections), outfile)

    # count: number of river sections to be examined. Total river sections is 304.
    count = riverSections["@iot.count"]

    #------------------------------------------
    # Mapping

    mapRS_df = mappingRS(riverSections)

    # excel
    # Store FWI dataframe to excel file
    xls = pd.ExcelWriter(directory + "/" + "mappingRS.xlsx")
    mapRS_df.to_excel(xls, 'Sheet1', index=False)
    xls.save()


    # End Timing Step 1
    end_step1 = time.time()
    time_duration_step.append( end_step1 - start_step1 )


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
    csv_fname =  root_path_dir + 'Amico_RS_in_Vicenza_v5.csv'
    IntRS = read_csv(csv_fname, sep=",")

    # Set constant variables which are utilised to create the query to extract Observations of each River Section
    #
    SensorThings = [SensorThingEntities[0], SensorThingEntities[1], SensorThingEntities[3], SensorThingEntities[5]]
    sel_vals = {'dstr_sel': ['id', 'name', 'properties'], 'obs_sel': ['result', 'phenomenonTime', 'id', 'parameters']}
    filt_args={'obs_filt': ['phenomenonTime']}

    # Define the Date/Time interval
    dates = ['2010-10-31T00:00:00.000Z', '2010-11-03T00:00:00.000Z']

    filt_vals={'obs_filt_vals': dates}
    ord_vals = ['phenomenonTime']

    #flag_last_run = True
    flag_last_run = False

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
    # and a vector of the scale cardinality (count = [n1, n2, n3, n4] for each group)
    # The river sections that belong to each of the groups are defined by the column RS_Group

    group_names = IntRS['RS_GroupName'].unique()
    group_ids = IntRS['RS_Group'].unique()
    group_descr = IntRS['RS_GroupDescr'].unique()

    # Initialization process to the list of dictionaries for RiverSect_CountScale
    RiverSect_CountScale = []
    for gr in range(len(group_names)):
        item = {'id': group_ids[gr], 'name': group_names[gr], 'descr':group_descr[gr],
                'count': [0,0,0,0], 'group_center_pos': [] }
        RiverSect_CountScale.append(item)

    #---------------------------------------------------------
    # Store forecast values to data.frame
    dfRSF = DataFrame([])

    TOTAL_TOPICS104_LIST = []

    flag_critical_rs = False

    for counter in range(0, count):

        print("\n Counter = ", counter)
        print("River Section Name: ", riverSections["value"][counter]['name'], ", ID: ", riverSections["value"][counter]['@iot.id'])

        if len( IntRS[ IntRS.ix[:,'Name'].str.contains(riverSections["value"][counter]['name']) ] ) != 0:

            total_irs_names = total_irs_names + 1

            # Find the position which RS has in the IntRS and determine whether it is a critical RS.
            # If it is, then flag_critical_rs will be equal to True.
            pos_rs = IntRS.loc[ IntRS.ix[:,'Name'].str.contains(riverSections["value"][counter]['name']) ].index.values

            if IntRS.ix[pos_rs[0], 'RS_Critical'] == 1:
                flag_critical_rs = True
                print("\n This RS is critical:", flag_critical_rs)
            else:
                print("\n This RS is NOT critical:", flag_critical_rs)

            # find the position of RiverSect_CountScale in which the river section name matches
            # with the name of group of river sections
            group_IntRS = int(IntRS[ IntRS['RS_ID_SensorThingServer'] == riverSections["value"][counter]['@iot.id'] ]['RS_Group'].iloc[0])

            # Take the name and the description of the group from the list RiverSect_CountScale
            for rscs_it in range(len(RiverSect_CountScale)):
                name_group = RiverSect_CountScale[RiverSect_CountScale[rscs_it]['id'] == group_IntRS]['name']

            # Arrays to store values from the TOP104 (initialize for each river section)
            max_yValues = []
            meas_color = []
            meas_note = []
            max_measurementID = []
            max_measurementTimeStamp = []
            dataSeriesID = []
            dataSeriesName = []
            xVals = []
            dataStreamName = []
            dataStreamID = []
            dataStreamDescript = []

            ids = {'th_id': str(riverSections["value"][counter]['@iot.id']) }

            if flag_last_run == False:
                response_forecast = extract_forecasts(service_root_URI, SensorThings, ids, sel_vals, ord_vals, filter_args=filt_args, filter_vals=filt_vals)
            else:
                response_forecast = extract_forecasts(service_root_URI, SensorThings, ids, sel_vals, ord_vals, last_run=flag_last_run)

            # write json (data) to output file
            #flname = directory + "/" + 'response_forecast_' + riverSections["value"][counter]['name'].replace(" ", "") + ".txt"
            #with open(flname, 'w') as outfile:
            #    json.dump(OrderedDict(response_forecast), outfile)

            #-----------------------------------------------
            # Update the data frame with new Observations
            Obs = response_forecast['Datastreams'][0]['Observations']
            Obs_df = DataFrame.from_dict(Obs)
            len_Obs = len(Obs_df)
            RS_name = DataFrame( [ riverSections["value"][counter]['name'] ]*len_Obs )
            RS_id = DataFrame( [ riverSections["value"][counter]['@iot.id'] ]*len_Obs )

            loc_riverSection = riverSections["value"][counter]['Locations'][0]['location']['coordinates']

            temp_df = concat( [ RS_name, RS_id ], axis=1)
            temp_df = concat( [ temp_df, Obs_df['phenomenonTime'], Obs_df['result'] ], axis=1)

            Obs_lat = DataFrame( [ loc_riverSection[1] ]*len_Obs )
            Obs_long = DataFrame( [ loc_riverSection[0] ]*len_Obs )
            temp_df = concat( [ temp_df, Obs_lat, Obs_long ], axis=1)

            dfRSF = concat( [dfRSF, temp_df] )

            #--------------------------------------------

            # Extract the thresholds of the response of riverSections query correspond to the specific river section
            thresh = [riverSections["value"][counter]['properties']['treshold1'],
                      riverSections["value"][counter]['properties']['treshold2'],
                      riverSections["value"][counter]['properties']['treshold3']]

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
            resp_comparison = compare_forecast_new_scale_thresholds(Obs_yv_max, thresh)

            flag_extreme = resp_comparison[len(resp_comparison) - 1]

            print("**** resp_comparison = ", resp_comparison)

            # Update the count in position equal with the scale adding one
            # for the particular group river section defined by name_group or group_IntRS
            RiverSect_CountScale[group_IntRS-1]['count'][ resp_comparison[2] - 1] += 1

            # Update the position of the group's center
            if len( RiverSect_CountScale[group_IntRS-1]['group_center_pos']) == 0:
                RiverSect_CountScale[group_IntRS-1]['group_center_pos'] = loc_riverSection
            #else:
            #    RiverSect_CountScale[group_IntRS-1]['group_center_pos'][0] += loc_riverSection[0]
            #    RiverSect_CountScale[group_IntRS-1]['group_center_pos'][1] += loc_riverSection[1]

            # For the cases which exceed one of the alarm thresholds do:
            if flag_extreme == True and resp_comparison[0][0] != '#00FF00':
                max_yValues += [Obs_yv_max]          # for forecast
                max_yValues += [resp_comparison[2]]  # for scale

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
                max_measurementTimeStamp += [datetime.utcnow().replace(microsecond=0).isoformat() + 'Z']*len(max_yValues)
                xVals += [ item['phenomenonTime'].replace('.000Z', "Z") ]*len(max_yValues)

                # --------------------------------------------------------------------------------------------
                #  STEP 2.2: Creates the TOPIC_104_METRIC_REPORT
                # ------------------------------------------------------------------------------------------
                #
                # Create the TOPIC 104 (json format) for the maximum value of predicted water levels
                # in the time interval defined by the 'dates' or for the lastRun of AMICO's program
                # for the specific river section.
                #
                # run and count Topic104index

                topics104 = topic104FloodIndex_VER14(directory, flag_last_run, response_forecast, max_yValues,
                                                    meas_color, meas_note, max_measurementID, max_measurementTimeStamp,
                                                    dataSeriesID, dataSeriesName, xVals, dataStreamName, dataStreamID,
                                                    dataStreamDescript, dates, thresh, riverSections,
                                                    RiverSect_CountScale, counter, mapRS_df)

                TOTAL_TOPICS104_LIST.append( topics104 )
                total_top104 = total_top104 + len(topics104)

                if flag_critical_rs == True:

                    topics104_critical = topic104FloodIndex_critical(directory, flag_last_run, response_forecast, max_yValues,
                                                                meas_color, meas_note, max_measurementID, max_measurementTimeStamp,
                                                                dataSeriesID, dataSeriesName, xVals, dataStreamName, dataStreamID,
                                                                dataStreamDescript, dates, thresh, riverSections,
                                                                RiverSect_CountScale, counter, mapRS_df)

                    # Append critical topis to the list
                    TOTAL_TOPICS104_LIST.append( topics104_critical )
                    total_top104 = total_top104 + len(topics104_critical)


        # Before consider the new RS, turn the flag_critical_rs to false again
        flag_critical_rs = False


    # End Timing Step 2
    end_step2 = time.time()
    time_duration_step.append( end_step2 - start_step2 )


    #-----------------------------------------------------
    print("\n len = ", len(TOTAL_TOPICS104_LIST))
    print("counter = ", total_top104)

    #==========================================================================
    # Start Timing Step 3 - Sending
    start_step3 = time.time()

    # Send messages to PSAP
    if len(TOTAL_TOPICS104_LIST) != 0:
        print(
            'Send message: Max Predicted Water Level value and its Category have been forwarded to logger into 2 separate messages!')

        for it in range(len(TOTAL_TOPICS104_LIST)):
            producer.send("TOP104_METRIC_REPORT", TOTAL_TOPICS104_LIST[it])
            #
            # print("\n ***** TOPIC: ")
            # print(top104_forecast[it])
            # print("*******\n")
    else:
        print('No messages will be forward to logger!!!')


    # End Timing Step 3 - Sending
    end_step3 = time.time()
    time_duration_step.append( end_step3 - start_step3 )


    #----------------------------------------------------------------------------------
    # write Data.Frame with forecasts to xlsx output file
    dfRSF.columns = ['RS_Name', 'RS_ID', 'phenomenonTime', 'result', 'Lat', 'Long' ]
    dfxls = ExcelWriter(directory + "/" + "DataFrame_forecasts.xlsx")
    dfRSF.to_excel(dfxls,'Sheet1', index=False)
    dfxls.save()

    #print("\n----- DATA FRAME ----- \n")
    #len(dfRSF)
    #print(dfRSF)
    #print("\n-------------------\n")

    # Update the center (position) of each group
    # for grid in range(len(RiverSect_CountScale)):
    #
    #     total_group_counts = sum(RiverSect_CountScale[grid]['count'])
    #
    #     if total_group_counts != 0:
    #         RiverSect_CountScale[grid]['group_center_pos'][0] = RiverSect_CountScale[grid]['group_center_pos'][0]/total_group_counts
    #         RiverSect_CountScale[grid]['group_center_pos'][1] = RiverSect_CountScale[grid]['group_center_pos'][1]/total_group_counts



    #-------------------------------------------------------------------------------------
    # STEP 4: Calculate the Overall Crisis Classification Index & Overall Crisis Level

    # Start Timing Step 4
    start_step4 = time.time()

    print("\n ======= RiverSect_CountScale BEFORE Calculate the OCCI \n")
    print(RiverSect_CountScale)

    # flag_scale = TRUE -> new scale is used {1,2,3,4}, otherwise the old scale is used {0,1,2,3}

    flag_scale = True
    over_crisis_class_indx = Overall_Crisis_Classification_Index(RiverSect_CountScale, flag_scale)

    print("\n***************************")

    print("\n ======= AFTER Calculate the OCCI \n")
    print("\n Groups River Sections Pre-Alert Overall Crisis Classification Index = ", over_crisis_class_indx)

    #------ OBSOLETE
    # OCL = Overall_Crisis_Level(RiverSect_CountScale, over_crisis_class_indx)
    #-----------------

    # Calculate the OCL per group and the OCL for all RS.
    # Use Weighted Average of the Overall Crisis Level over all groups

    weights = [1]*len(RiverSect_CountScale)

    OCL = Group_Overall_Crisis_Level(RiverSect_CountScale, over_crisis_class_indx, weights)

    print("\n***************************")

    print("\n Pre-Alert Overall Crisis Lever = ", OCL)
    print("\n***************************\n")


    # ----------------------------------------------------------------------------------------------
    # Creates TOP104 for the Overall_Crisis_Level index per group of river sections
    # and the whole region of interest

    total_top104_index = 0

    total_topic104_overall = topic104FloodOverall(directory, RiverSect_CountScale, OCL, total_top104_index, producer)

    total_top104 = total_top104 + total_topic104_overall


    # End Timing Step 4
    end_step4 = time.time()
    time_duration_step.append( end_step4 - start_step4 )


    #---------------------------------------------------------------------------
    total_time = np.array(time_duration_step).sum()

    print("\n ****** EXECUTION TIME: **** ")
    print(" Time for Step 1. Data Acquisition: ", round(time_duration_step[0], 3), " seconds")
    print(" Time for Step 2. Calculate WL scale create Topics 104 for River Sections: ", round(time_duration_step[1], 3), " seconds")
    print(" Time for Step 3. Sending messages: ", round(time_duration_step[2], 3), " seconds")
    print(" Time for Step 4. Calculate OCCI & PFLCL: ", round(time_duration_step[3], 3), " seconds")
    print(" Total Execution Time: ", round(total_time/60.0, 3), " minutes")

    print("\n Total interested River Sections = ", total_irs_names)
    print(" Number of TOP104 which were sent to PSAP is: ", total_top104)
    print(" ************************** \n")


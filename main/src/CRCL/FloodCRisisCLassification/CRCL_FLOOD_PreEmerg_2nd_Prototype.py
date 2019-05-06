# Created Date: 03/05/2019
# Modified Date:
#
# -------------------------------- 2nd PROTOTYPE RELEASE VERSION BASED ON PILOT FLOOD VICENZA -------------------------------------------
#       Version with Risk Maps
#
# Implements the algorithm of Early Warning Crisis Classification module
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
# Outputs: TOP104_METRIC_REPORT which contains (pre-alert visualization):
#          a) the maximum predicted crisis level in the next 54h for
#           the particular number of river sections (predefined 60 RS)
#          b) topics for the gauge plots Overall Crisis Level per group & whole region
#          c) topics for line plots indicating the WL at forecasting period over 6
#           pre-defined Critical River Sections
#
#   Algorithm 1 from Crisis Classification (based on AAWA)
#----------------------------------------------------------------------------------------------------------
#

from bus.bus_producer import BusProducer
import json, time, re
import os, errno
from pathlib import Path
from pandas import read_csv, DataFrame, concat, ExcelWriter, read_excel
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from math import pow, ceil
from collections import OrderedDict

import pymysql

from CRCL.FloodCRisisCLassification.Topic104_Metric_Report import Top104_Metric_Report
from CRCL.FloodCRisisCLassification.Create_Queries import *
from CRCL.FloodCRisisCLassification.Create_Queries import extract_river_sections_loc

from CRCL.FloodCRisisCLassification.Auxiliary_functions import *
from CRCL.FloodCRisisCLassification.topic104Flood import *

from CRCL.FloodCRisisCLassification.Topic106_Risk_Maps import *
from CRCL.FloodCRisisCLassification.topic106_RiskMaps import *

from CRCL.FloodCRisisCLassification.Auxiliary_Functions_DB import *


#---------------------------------------------------------------------------------------------------------------
def CrisisClassificationFlood_PreEmerg_v2(cur_date, flag_mode, session):

    tp6 = 0
    tp41 = 0
    tp42 = 0
    ver = 'PreEmerg_2nd_PROTOTYPE'

    # Create a directory to store the output files and TOPICS
    # Create a path
    current_dirs_parent = os.getcwd()
    root_path_dir = current_dirs_parent + "/" + "CRCL/FloodCRisisCLassification" + "/"

    now = datetime.now()
    #directory = root_path_dir + "TOPICS_for_2010" + "_" + ver + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day)
    directory = root_path_dir + ver + "_" + "TOPICS" + "_" + flag_mode + "_" + session + "_" + cur_date.isoformat()
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

    # Read the new excel files for the pilot
    if session == '4March':
        xlsx_name = root_path_dir + 'Forecasts_4March_1stThres.xlsx'
        df_Forecasts_March = pd.read_excel(xlsx_name, sheet_name=session)
    elif session == '5March':
        xlsx_name = root_path_dir + 'Forecasts_5March_2ndThres.xlsx'
        df_Forecasts_March = pd.read_excel(xlsx_name, sheet_name=session)
    elif session == '6March':
        xlsx_name = root_path_dir + 'Forecasts_6March_3rdThres.xlsx'
        df_Forecasts_March = pd.read_excel(xlsx_name, sheet_name=session)

    # Convert DataTime by 18h ahead
    tempTime = DataFrame(df_Forecasts_March['phenomenonTime'].apply(lambda x: x + timedelta(hours=18)))
    tempTime.reset_index(drop=True, inplace=True)

    df_Forecasts_March = df_Forecasts_March.drop('phenomenonTime', 1)
    df_Forecasts_March['phenomenonTime'] = tempTime

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
    csv_fname = root_path_dir + 'Amico_RS_in_Vicenza_v6.csv'

    ################## AMICO RIVER STATIONS TO SQL DATA BASE ###############################

    # CREATE CRITICAL STATION TABLE AND LOAD DATA
    RS_file = "'" + str(root_path_dir) + "Amico_RS_in_Vicenza_v6.csv'"
    Database = 'Flood_CRCL_DB'
    RS_Table = '`Interest_RiverSections`'
    Entries_RS = [{'colname': "`RS_ID_AAWA`", 'type': 'VARCHAR(255)', 'check_null': 'NOT NULL', 'PRIMARY KEY': True},
                  {'colname': "`Name`", 'type': 'VARCHAR(255)', 'check_null': 'NOT NULL', 'PRIMARY KEY': False},
                  {'colname': "`RS_ID_SensorThingServer`", 'type': 'INT UNSIGNED', 'check_null': 'NOT NULL',
                   'PRIMARY KEY': False},
                  {'colname': "`RS_Critical`", 'type': 'INT UNSIGNED', 'check_null': 'NOT NULL', 'PRIMARY KEY': False},
                  {'colname': "`RS_Group`", 'type': 'INT UNSIGNED', 'check_null': 'NOT NULL', 'PRIMARY KEY': False},
                  {'colname': "`RS_GroupName`", 'type': 'VARCHAR(255)', 'check_null': 'NOT NULL', 'PRIMARY KEY': False},
                  {'colname': "`RS_GroupDescr`", 'type': 'TEXT', 'check_null': 'NOT NULL', 'PRIMARY KEY': False}]

    ######## check if table exists #########
    conn = pymysql.connect(host='localhost', user='root', password='', db=Database)
    curs = conn.cursor()

    Show_sql_RS = "SHOW TABLES LIKE 'Interest_RiverSections'"
    curs.execute(Show_sql_RS)
    result_RS = curs.fetchall()

    if result_RS:
        print("There is already a table in the database named " + RS_Table)

        # Check if table has entries
        Data_sql_RS = 'SELECT * FROM ' + RS_Table
        num_of_rows_RS = curs.execute(Data_sql_RS)

        print('Number of rows in the ' + RS_Table + ": " + str(num_of_rows_RS))

        #### Table exists, but no data
        if num_of_rows_RS == 0:
            print("Loading data:")
            load_data_from_file(Database, RS_Table, RS_file)
        else:
            print("No data is going to load!!!")

    else:
        print("There is a no table named " + RS_Table)
        print(result_RS)

        #### Create the table if it doesnt exist ####
        print("Create Table:")
        create_table(Database, RS_Table, Entries_RS)

        #### Load the Data #####
        print("Loading data")
        load_data_from_file(Database, RS_Table, RS_file)


    # Get the data from the database
    sql = 'SELECT * FROM `Interest_RiverSections`'
    num_of_rows = curs.execute(sql)
    print('Number of rows ' + str(num_of_rows))

    # Fetch all the rows
    data_RS = curs.fetchall()

    colnames = ['RS_ID_AAWA', 'Name', 'RS_ID_SensorThingServer', 'RS_Critical', 'RS_Group',
                'RS_GroupName', 'RS_GroupDescr']

    IntRS = pd.DataFrame(list(data_RS), columns=colnames)

    ########################################################################################
    #	
    # Set constant variables which are utilised to create the query to extract Observations of each River Section
    #
    # SensorThings = [SensorThingEntities[0], SensorThingEntities[1], SensorThingEntities[3], SensorThingEntities[5]]
    # sel_vals = {'dstr_sel': ['id', 'name', 'properties'], 'obs_sel': ['result', 'phenomenonTime', 'id', 'parameters']}
    # filt_args={'obs_filt': ['phenomenonTime']}
    #
    # # Define the Date/Time interval
    # #
    # # If flag_mode = 'Fake' then CRCL utilises old flood data since 2010.
    if flag_mode == 'Fake':
        dates = ['2010-10-31T00:00:00.000Z', '2010-11-03T00:00:00.000Z']
    else:
        iso_cur_date = cur_date.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        end_date = cur_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=3)
        iso_end_date = end_date.isoformat()

        dates = [iso_cur_date + '.000Z', iso_end_date + '.000Z']

    # print(" <<<< ", dates, " >>>> \n")
    #
    # filt_vals={'obs_filt_vals': dates}
    # ord_vals = ['phenomenonTime']

    #flag_last_run = True
    flag_last_run = False

    #----------------------------------------------------------------------------------------
    # Create new Producer instance using provided configuration message (dict data).

    producer = BusProducer()

    # Decorate terminal
    print('\033[95m' + "\n***********************")
    print("*** CRCL SERVICE v1.0 ***")
    print("***********************\n" + '\033[0m')

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
    TOTAL_TOPICS106_LIST = []

    TOPICS106_TIME = []
    polygons_id = []

    total_irs_names = 0
    total_critical_rs = 0
    total_top104 = 0

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
                if RiverSect_CountScale[rscs_it]['id'] == group_IntRS:
                    name_group = RiverSect_CountScale[rscs_it]['name']

            # Arrays to store values from the TOP104 (initialize for each river section)
            max_yValues = []
            max_xValues = []
            meas_color = []
            meas_note = []
            meas_category = []
            max_measurementID = []
            max_measurementTimeStamp = []
            dataSeriesID = []
            dataSeriesName = []
            yValues = []
            xValues = []
            dataStreamName = []
            dataStreamID = []
            dataStreamDescript = []

            # Extract the thresholds of the response of riverSections query correspond to the
            # specific river section

            thresh = [{"note": "Alarm Threshold 1", "xValue": "1",
                       "yValue": riverSections["value"][counter]['properties']['treshold1'],
                       "color": "#F8E71C"},
                      {"note": "Alarm Threshold 2", "xValue": "2",
                       "yValue": riverSections["value"][counter]['properties']['treshold2'],
                       "color": "#E68431"},
                      {"note": "Alarm Threshold 3", "xValue": "3",
                       "yValue": riverSections["value"][counter]['properties']['treshold3'],
                       "color": "#AA2050"}
                      ]

            #-----------------------------------------------
            # Update the data frame with new Observations
            # Obs = response_forecast['Datastreams'][0]['Observations']
            # Obs_df = DataFrame.from_dict(Obs)
            # len_Obs = len(Obs_df)

            ids = {'th_id': riverSections["value"][counter]['@iot.id'] }

            Obs_df = df_Forecasts_March[df_Forecasts_March['RS_ID'] == ids['th_id']]

            # Modify the format of the phenomenonTime in all the corresponding column of Obs_df
            tempTime = DataFrame(Obs_df['phenomenonTime'].apply(lambda x: x.isoformat() + '.000Z'))
            Obs_df = Obs_df.drop('phenomenonTime', 1)
            Obs_df['phenomenonTime'] = tempTime

            Obs_df.reset_index(drop=True, inplace=True)

            len_Obs = len(Obs_df)
            # print("\n lenObs= ", len_Obs)

            if len_Obs > 0:

                RS_name = DataFrame( [ riverSections["value"][counter]['name'] ]*len_Obs )
                RS_name.reset_index(drop=True, inplace=True)

                RS_id = DataFrame( [ riverSections["value"][counter]['@iot.id'] ]*len_Obs )
                RS_id.reset_index(drop=True, inplace=True)

                loc_riverSection = riverSections["value"][counter]['Locations'][0]['location']['coordinates']

                # RS_df = concat( [ RS_name, RS_id ], axis=1)
                # RS_df = concat( [ RS_df, Obs_df['phenomenonTime'], Obs_df['result'] ], axis=1)

                RS_df = concat([RS_name, RS_id], axis=1, ignore_index=True)
                RS_df = concat([RS_df, Obs_df['phenomenonTime'], Obs_df['Result'] ], axis=1, ignore_index=True)

                Obs_lat = DataFrame( [ loc_riverSection[1] ]*len_Obs )
                Obs_lat.reset_index(drop=True, inplace=True)

                Obs_long = DataFrame( [ loc_riverSection[0] ]*len_Obs )
                Obs_long.reset_index(drop=True, inplace=True)

                RS_df = concat( [ RS_df, Obs_lat, Obs_long ], axis=1, ignore_index=True)

                RS_df.columns= [ 'RS_Name', 'RS_ID', 'phenomenonTime', 'result', 'Lat', 'Long']

                # Calculate the scale and color of the WL forecasting values for each time
                # comparing them by the alarm thresholds
                thresh_yVals = [ thresh[0]['yValue'], thresh[1]['yValue'],thresh[2]['yValue'] ]

                comp_RS_df = compare_forecasts_df_thresholds_PILOT(RS_df, thresh_yVals)

                # Append to the unified data frame (dfRSF) the entries of the data frame for each River Section
                dfRSF = concat( [dfRSF, comp_RS_df], ignore_index=True)

                #--------------------------------------------
                # Extract the observations WL forecasted values and stored in the array yValues
                #
                Obs_yv = comp_RS_df['result']
                Obs_yv_max = comp_RS_df['result'].max()

                maxIndexList = [i for i, j in enumerate(Obs_yv) if j == Obs_yv_max]
                first_max_pos = min(maxIndexList)  # considers only the first maximum value

                # print( "\n first_max_pos = ", first_max_pos)

                # Calculate the Crisis Classification Level for the specific River Section
                #   If the maximum value exceeds one of the predefined thresholds then
                #   it stored in the topic (flag_extreme=True), otherwise it ignores (flag_extreme = False)
                #
                # resp_comparison = compare_forecast_new_scale_thresholds(Obs_yv_max, thresh)

                resp_comparison = {"Measurement_Color": comp_RS_df.loc[first_max_pos, "Measurement_Color"],
                                    "Measurement_Note": comp_RS_df.loc[first_max_pos, "Measurement_Note"],
                                    "Scale": comp_RS_df.loc[first_max_pos, "Scale"],
                                    "Scale_Note": comp_RS_df.loc[first_max_pos, "Scale_Note"],
                                    "Flag_Extreme": comp_RS_df.loc[first_max_pos, "Flag_Extreme"]
                                   }

                # print( "\n resp_comparison = ", resp_comparison)

                flag_extreme = resp_comparison["Flag_Extreme"]

                # print("**** resp_comparison (1st max) = ", resp_comparison)

                # Update the count in position equal with the scale adding one
                # for the particular group river section defined by name_group or group_IntRS
                # RiverSect_CountScale[group_IntRS-1]['count'][ resp_comparison[2] - 1] += 1

                RiverSect_CountScale[group_IntRS-1]['count'][ resp_comparison['Scale'] -1 ] += 1

                # Update the position of the group's center
                if len( RiverSect_CountScale[group_IntRS-1]['group_center_pos']) == 0:
                    RiverSect_CountScale[group_IntRS-1]['group_center_pos'] = loc_riverSection
                #else:
                #    RiverSect_CountScale[group_IntRS-1]['group_center_pos'][0] += loc_riverSection[0]
                #    RiverSect_CountScale[group_IntRS-1]['group_center_pos'][1] += loc_riverSection[1]

                # For the cases which exceed one of the alarm thresholds do:
                # if flag_extreme == True and resp_comparison['Measurement_Color'] != '#00FF00':

                if flag_extreme == True:

                    max_yValues += [Obs_yv_max]          # for forecast
                    max_yValues += [ int(resp_comparison['Scale']) ]  # for scale

                    meas_color.append( resp_comparison['Measurement_Color'] )   # for forecast
                    meas_color.append("")                        # for scale

                    meas_note.append( resp_comparison["Measurement_Note"] )    # for forecast
                    meas_note.append( resp_comparison["Scale_Note"] )    # for scale

                    # Category is new field. Its values is "Low", ..."Very High"
                    meas_category.append( resp_comparison["Scale_Note"] ) # for forecast
                    meas_category.append( resp_comparison["Scale_Note"] ) # for scale

                    dataSeriesID += [riverSections["value"][counter]['@iot.id']]*len(max_yValues)  # counter + 1
                    dataSeriesName += [riverSections["value"][counter]['name']]*len(max_yValues)

                    # Find details regarding the maximum observation and stored them in the corresponding arrays
                    # item = response_forecast['Datastreams'][0]['Observations'][first_max_pos]
                    # max_measurementID += [ str(item['@iot.id']) + '_1' ] # for forecast
                    # max_measurementID += [ str(item['@iot.id']) + '_2' ] # for scale

                    # phnTime = item['phenomenonTime'].replace('.000Z', "Z")
                    # max_measurementTimeStamp += [phnTime] * len(max_yValues)
                    # max_xValues += [phnTime] * len(max_yValues)

                    measID =  str( round(datetime.utcnow().timestamp() * 1000) )
                    max_measurementID += [ measID + '1' ]
                    max_measurementID += [ measID + '2' ]

                    phnTime = comp_RS_df.loc[first_max_pos, "phenomenonTime"].replace('.000Z', "Z")
                    max_measurementTimeStamp += [ phnTime ]*len(max_yValues)
                    max_xValues += [ phnTime ]*len(max_yValues)

                    # --------------------------------------------------------------------------------------------
                    #  STEP 2.2: Creates the TOPIC_104_METRIC_REPORT
                    # ------------------------------------------------------------------------------------------
                    #
                    # Create the TOPIC 104 (json format) for the maximum value of predicted water levels
                    # in the time interval defined by the 'dates' or for the lastRun of AMICO's program
                    # for the specific river section.
                    #
                    # run and count Topic104index

                    # Create the Topic if and only if the forecasted Water level exceed the 1st alarm threshold
                    #
                    if resp_comparison['Measurement_Color'] != '#31A34F':

                        topics104 = topic104FloodIndex_Map_v2(directory, flag_last_run, max_yValues,
                                                            meas_color, meas_note, meas_category, max_measurementID, max_measurementTimeStamp,
                                                            dataSeriesID, dataSeriesName, max_xValues, dataStreamName, dataStreamID,
                                                            dataStreamDescript, dates, thresh, riverSections,
                                                            RiverSect_CountScale, counter, mapRS_df)


                        TOTAL_TOPICS104_LIST.append( topics104[0] )
                        # Check if the list has content and send to the PSAP
                        if len(TOTAL_TOPICS104_LIST) != 0:

                            print('Send message: Max Predicted Water Level value and its Category have been forwarded to logger!')
                            print("\n ***** TOPICS 104: ")
                            producer.send("TOP104_METRIC_REPORT", topics104[0])
                            tp41 = tp41+1

                            print("*************************************\n")
                        else:
                            print('No messages will be forward to logger!!!')

                        total_top104 = total_top104 + len(topics104)

                        #****************************************************************************************************
                        # Create TOP106 from Risk Maps
                        start_step_polygons = time.time()

                        row_mapRS_df = mapRS_df.index[ mapRS_df['SensorID'] == riverSections["value"][counter]['@iot.id'] ][0]

                        # Position of interest -> River Section
                        RS_position = {'long': round(mapRS_df['Long'].iloc[row_mapRS_df], 5),
                                        'lat': round(mapRS_df['Lat'].iloc[row_mapRS_df], 5) }

                        # bbox size to request for polygons at Risk Maps
                        bbox_size = {'width': 0.001, 'height': 0.0075}

                        # Name of the file in geoserver so as to check for polygons
                        geoserver_name = 'awaa_risk_TR100'
                        # geoserver_name = 'awaa_risk_TR300'

                        response_q = bbox_query_riskmaps(geoserver_name, RS_position, bbox_size)

                        if len(response_q['features']) > 0:

                            features_list = extract_features(response_q)

                            rivSec_name = riverSections["value"][counter]['name']

                            # Create Topics 106
                            polygons_topics106, li = topic106_create_RiskMap( directory, rivSec_name, RS_position, features_list, polygons_id)
                            polygons_id = li


                            # Add Top106 to the list of Topics 106 over all RS
                            # Send the topics
                            if polygons_topics106[ 'Topic_Polygons' ] != 'No Topic':

                                TOTAL_TOPICS106_LIST.append( polygons_topics106[ 'Topic_Polygons' ] )

                                if len(TOTAL_TOPICS106_LIST) != 0:
                                    print('Send message: Polygons of Risk Map with RT 100 years have been forwarded to logger!')
                                    print("\n ***** TOPICS 106 Polygons: ")
                                    producer.send("TOP106_RISK_MAPS", polygons_topics106[ 'Topic_Polygons' ])
                                    tp6 = tp6 + 1
                                    print("**************************************\n")

                            else:
                                print('No messages will be forward to logger!!!')

                        else:
                             print("\n No polygons has been found in the specific bbox around the River Section ",
                                   riverSections["value"][counter]['name'])

                        stop_step_polygons = time.time()
                        TOPICS106_TIME.append(stop_step_polygons - start_step_polygons)

                    else:
                        print("\n All the forecast Water Level of this RS ", riverSections["value"][counter]['name'],
                                    " did not exceed the Alarm Thresholds. No message will be created to send !!! \n")

                    #-----------------------------------------------------------------------------------------------------------
                    # Create Topic 104 for Critical River Sections for the dashboard
                    #
                    if flag_critical_rs == True:

                        total_critical_rs = total_critical_rs + 1

                        # Topic for Dashboard - linePlot Critical River Sections
                        #
                        topics104_critical_linePlot = topic104FloodIndex_critical_linePlot(directory, flag_last_run,
                                                                                           comp_RS_df, thresh, riverSections,
                                                                                           max_measurementTimeStamp, dates,
                                                                                           counter, mapRS_df)

                        TOTAL_TOPICS104_LIST.append(topics104_critical_linePlot)
                        print('Sending the Critical LinePlot to PSAP: ')
                        producer.send("TOP104_METRIC_REPORT", topics104_critical_linePlot)
                        tp42 = tp42 +1

                        total_top104 = total_top104 + len(topics104_critical_linePlot)

            else:
                print("\n NO OBSERVATIONS have stored for the River Section: ", riverSections["value"][counter]['name'])

        # Before consider the new RS, turn the flag_critical_rs to false again
        flag_critical_rs = False

    print("\n Check reasons =>> total_top104 = ", total_top104)
    print("Check reasons =>> total_critical_rs = ", total_critical_rs)
    print("Check reasons =>> total_irs_names = ", total_irs_names)

    # End Timing Step 2
    end_step2 = time.time()
    time_duration_step.append( end_step2 - start_step2 )

    # Start Timing Step 3
    start_step3 = time.time()

    #-----------------------------------------------------
    print("\n len = ", len(TOTAL_TOPICS104_LIST))
    print("counter top104= ", total_top104)

    print("\n len Polygon topics 106= ", len(TOTAL_TOPICS106_LIST))

    # ----------------------------------------------------------------------------------
    # write Data.Frame with forecasts to xlsx output file
    if len(TOTAL_TOPICS104_LIST) != 0:
        print("\n Store DataFrame to excel file \n")
        dfRSF.columns = ['RS_Name', 'RS_ID', 'phenomenonTime', 'result', 'Lat', 'Long',
                         'Measurement_Color', 'Measurement_Note','Scale', 'Scale_Note', 'Flag_Extreme']
        dfxls = ExcelWriter(directory + "/" + "DataFrame_forecasts.xlsx")
        dfRSF.to_excel(dfxls, 'Sheet1', index=False)
        dfxls.save()
    else:
        print('No entries to store in Excel file!!!')

    # -------------------------------------------------------------------------------------
    # STEP 4: Calculate the Overall Crisis Classification Index & Overall Crisis Level

    print("\n ======= RiverSect_CountScale BEFORE Calculate the OCCI \n")
    print(RiverSect_CountScale)

    if len(TOTAL_TOPICS104_LIST) != 0:

        # flag_scale = TRUE -> new scale is used {1,2,3,4}, otherwise the old scale is used {0,1,2,3}
        flag_scale = True
        over_crisis_class_indx = Overall_Crisis_Classification_Index(RiverSect_CountScale, flag_scale)

        print("\n***************************")
        print("\n ======= AFTER Calculate the OCCI \n")
        print("\n Groups River Sections Pre-Alert Overall Crisis Classification Index = ", over_crisis_class_indx)

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

        # ----------------------------------------------------------------------------------------------------------------------
        # Creates TOP104 for the Distribution of Predicted Flood Crisis Level for all of the River Sections in the Municipality
        # The results are presented in the Traffic light at the Dashboard

        # Calculate the distribution of the crisis level (scale) over all interest RS
        sum_CLRS = 0

        for i in range(len(RiverSect_CountScale)):
            sum_CLRS = sum_CLRS + np.array(RiverSect_CountScale[i]['count'])

        # Create an array of dictionaries
        TR_CLRS = []
        categories_tr = ['Low', 'Medium', 'High', 'Very High']
        colors_tr = ["#00FF00", "#FFFF00", "#FFA500", "#FF0000"]
        for i in range(len(sum_CLRS)):
            item_tr = {'TrL_ID': 'RS_DPCL_ID_1001',
                       'TrL_name': 'All R.S. in Municipality',
                       'TrL_value': sum_CLRS[i],
                       'TrL_color': colors_tr[i],
                       'TrL_note': categories_tr[i]}

            TR_CLRS.append(item_tr)

        print("\n Checking reasons TR_CLRS: ", TR_CLRS)

        topic104_traffic = topic104Flood_Traffic_Dashboard(directory, TR_CLRS, producer)

        total_top104 = total_top104 + topic104_traffic

        # ---------------------------------------------------------------------------------------------------------
        # Creates TOP104 for the Distribution of Predicted Flood Crisis Level at each one of the River Reach (group)
        # The results are presented in the Pie Charts at the Dashboard
        #

        topic104_pie = topic104Flood_PieChart_Dashboard(directory, RiverSect_CountScale, producer)

        print(topic104_pie)

    else:
        print('No messages for Overall Crisis Level index will be forward to logger!!!')

    # End Timing

    end_step3 = time.time()
    time_duration_step.append( end_step3 - start_step3 )

    # ---------------------------------------------------------------------------
    total_time = np.array(time_duration_step).sum()
    total_time_polygons = np.array(TOPICS106_TIME).sum()

    print("\n ****** EXECUTION TIME: **** ")

    Data_Acquisition = round(time_duration_step[0], 3)
    Data_Acquisition_min = round(time_duration_step[0]/ 60.0, 3)
    print(" Time for Step 1. Data Acquisition: ", Data_Acquisition, " seconds")

    Calculations_Topics = round(time_duration_step[1], 3)
    Calculations_Topics_min = round(time_duration_step[1] / 60.0, 3)
    print(" Time for Step 2. Calculate WL scale create Topics 104/106 for River Sections and for Risk Maps, and send them to the PSAP : ",Calculations_Topics, " seconds")

    Polygon_Time = round(total_time_polygons, 3)
    Polygon_Time_min = round(total_time_polygons/ 60.0, 3)
    print(" Time for Calculating and forwarding the Polygons: ", Polygon_Time, " seconds")

    Overalls_Time = round(time_duration_step[2], 3)
    Overalls_Time_min = round(time_duration_step[2]/ 60.0, 3)
    print(" Time for Step 3. Calculate OCCI & PFLCL: ", Overalls_Time, " seconds")

    Full_Time = round( (total_time + total_time_polygons)/ 60.0, 3)
    Full_Time_secs = round( (total_time + total_time_polygons), 3)
    print(" Total Execution Time: ", Full_Time, " minutes")

    # LIST OF TIMES
    Timers_lists = [['Data Acquisition', Data_Acquisition, Data_Acquisition_min], ['WL and Topics', Calculations_Topics, Calculations_Topics_min],
                    ['Risk Maps', Polygon_Time, Polygon_Time_min], ['Overalls OCCI/PFLCL', Overalls_Time,Overalls_Time_min],
                    ['Total Time', Full_Time_secs, Full_Time]
                    ]

    Timers = pd.DataFrame(Timers_lists, columns = ['Timer', 'Seconds', 'Minutes'])

    Counter_lists = [ ['Total TOP104 ', total_top104], ['Topics 104 for WL', tp41],
                      ['Topics 104 Critical Line Plot ', tp42],
                      ['Topics 104 Overalls OCCI/PFLCL', total_top104 - tp41 - tp42],
                      ['TOPICS 106 for Risk Maps ', tp6],
                      ['Number of unique polygons', len(polygons_id)]
                    ]

    Counters = pd.DataFrame(Counter_lists, columns = ['Description', 'Number'])

    xlsEv = pd.ExcelWriter(directory + "/" + "Evaluation_Results.xlsx")
    Timers.to_excel(xlsEv, 'Evaluation_Time', index=False)
    Counters.to_excel(xlsEv,'Evaluation_Counter',index=False)
    xlsEv.save()

    print("\n Total interested River Sections = ", total_irs_names)
    print(" Number of TOP104 which were sent to PSAP is: ", total_top104)
    print(" TOPICS 104 WL Forwarded to the PSAP = ", tp41)
    print(" TOPICS 104 Critical Line Plot Forwarded to the PSAP = ", tp42)
    print(" Other statistical TOPICS 104 which forward to PSAP = ", total_top104 - tp41 - tp42)

    print(" ************************** \n")
    print(" TOPICS 106 Forwarded to the PSAP = ", tp6)
    print(" Name of TR file is:", geoserver_name )
    print('ID of polygons:', polygons_id)
    print('Number of unique polygons:', len(polygons_id))

    print("\n This task has been completed!!! CRCL flood pre-emergency phase waits for another trigger message!!! \n")
# Created Date: 03/05/2019
# Modified Date:
#
# -------------------------------- 2nd PROTOTYPE VERSION - SIMULATED DATA EMERGENCY PHASE AUTORUN --------------------------------------------------
#
#   During the Heatwave crisis. Simulated weather data
#

from bus.bus_producer import BusProducer

import json, time, re
import os, errno
from pathlib import Path
import pandas as pd
from pandas import read_csv, DataFrame, concat, ExcelWriter, read_excel
from datetime import datetime, timedelta
from collections import OrderedDict

from CRCL.HeatwaveCRisisCLassification.Top104_Metric_Report import Top104_Metric_Report
from CRCL.HeatwaveCRisisCLassification.topic104HeatWave import *
from CRCL.HeatwaveCRisisCLassification.Auxiliary_functions import *
from CRCL.HeatwaveCRisisCLassification.Create_Queries_HeatwavePilot import *

from CRCL.HeatwaveCRisisCLassification.Plots_HW import *
from CRCL.HeatwaveCRisisCLassification.parse_XML_dict import parse_json_to_dict_querylist

import urllib.request
import xmltodict
import numpy as np

def CrisisClassificationHeatwave_FakeData_Emerg_v2(iteration, flag_location_status, flag_HOCL, CategThresh):

    ver = 'Emerg_FakeData_2nd_PROTOTYPE_AUTORUN'

   # Create a directory to store the output files and TOPICS
    current_dirs_parent = os.getcwd()
    root_path_dir = current_dirs_parent + "/" + "CRCL/HeatwaveCRisisCLassification" + "/"

    now = datetime.now()
    folder_name = "TOPICS_HW_EMERG_FakeData_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + "/"

    directory = root_path_dir + folder_name + "TOPICS" + "_" + ver + "_" + str(iteration) + "_" + now.replace(microsecond=0).isoformat()
    os.makedirs(directory, exist_ok=True)

    # Start Timing Step 1
    start_step1 = time.time()

    # Store the time steps
    time_duration_step = []

    # --------------------------------------------------------------------------------------------------
    # STEP 1: Fetch forecasting data from the Dark Sky API
    #
    # Query to extract forecasting weather data from a set of points in Thessaloniki region
    #
    points = [{'name': 'Euosmos', 'lat': 40.664281, 'long': 22.898388},
              {'name': 'Aristotelous Sq.', 'lat': 40.632903, 'long': 22.940400},
              {'name': 'Faliro', 'lat': 40.619788, 'long': 22.957993},
              {'name': 'Konstantinopolitika', 'lat': 40.611343, 'long': 22.992180},
              {'name': 'Thermi-Xortiatis', 'lat': 40.581375, 'long': 23.097996},
              {'name': 'Airport', 'lat': 40.514354, 'long': 22.986642}
              ]

    # End Timing Step 1
    end_step1 = time.time()
    time_duration_step.append(end_step1 - start_step1)

    #---------------------------------------------------------------------------------------
    # STEP 2: Call function to parse JSON with real observations and extract the data

    # Fake Data
    print(" STEP 2: Parse JSON with real observations to extract the data... ")
    print("\n Fetch observation data from the excel file ...")
    # Start Timing Step 2
    start_step2 = time.time()

    fl_xlsx_path = root_path_dir + 'Simulated_Data_3min_v2.1.xlsx'
    dset = pd.read_excel(fl_xlsx_path)

    # Convert Date/Time to current date/time
    result = convert_datetime_to_current(dset)

    # Write results to Excel file
    fln = "CURRENT_DateTime__Simulated_Data_3min_v2.1.xlsx"
    dfxls = pd.ExcelWriter(directory + "/" + fln)
    result.to_excel(dfxls, 'Sheet1', index=False)
    dfxls.save()

    # From the whole dataset keep only the records that correspond to the
    # particular iteration => specific date/time slot
    #
    dset_iter = find_df_of_nearest_time(result)

    # End Timing Step 2
    end_step2 = time.time()
    time_duration_step.append(end_step2 - start_step2)

    # --------------------------------------------------------------------------------------------------
    # STEP 3: Calculates the Discomfort Index (DI) for heatwave based on Temperature and Humidity
    #---------------------------------------------------------------------------------------------------

    print(" STEP 3: Calculates the Discomfort Index (DI)...")

    # Start Timing Step 3
    start_step3 = time.time()

    # constant threshold for temperature variable (from papers ~ 14.5)
    InitTemp = 14.5
    model_param = 0.55

    # Fake Data
    DSDI = Heatwave_Disconfort_Index(dset_iter, InitTemp, model_param)


    # Write results to Excel file
    fln = "DSDI_Simulated_RealTime_v2_Iteration_" + str(iteration) + ".xlsx"
    dfxls = pd.ExcelWriter(directory + "/" + fln)
    DSDI.to_excel(dfxls, 'Sheet1', index=False)
    dfxls.save()

    # --------------------------------------------------------------------------------------------
    #  STEP 3.1: Calculate the Heatwave Overall Crisis Level per day over the 6 points
    # --------------------------------------------------------------------------------------------
    #

    print(" STEP 3.1: Calculates the Heatwave Overall Crisis Level per day over the 6 areas...")

    HOCL = []

    # Center of the points of interest
    center_points = []

    N = float(len(points))
    avglat = 0
    avgln = 0
    for p in points:
        avglat = avglat + p['lat']
        avgln = avgln + p['long']

    # Long/Lat array contains the coordinates of the city center
    center_points = [round(avgln / N, 5), round(avglat / N, 5)]

    # list of days
    days = []
    unique_dates = list(DSDI['DateTime'].unique())
    for i in range(len(unique_dates)):
        days.append(unique_dates[i].split('T')[0].split('-')[2])

    days = list(set(days))

    # Call the function Heatwave_Overall_Crisis_Level
    item_dict = Heatwave_Overall_Crisis_Level(DSDI)

    item_dict.update({'DateTime': DSDI['DateTime'].iloc[0] })
    item_dict.update({'Position': center_points})

    HOCL.append(item_dict)

    print(HOCL)

    # Check for HOCL values that are above the threshold and update flags
    # DI categories that include to the overall topic creation
    valid_values = ['Normal', 'Warm', 'Hot', 'Very Hot', 'Extreme']

    HOCL_to_topic = []

    for hocl_it in range(len(HOCL)):

        if HOCL[hocl_it]['note'] in valid_values:

            if flag_HOCL[hocl_it]['flag_Status'] == False:
                flag_HOCL[hocl_it]['flag_Status'] = True
                HOCL_to_topic.append(HOCL[hocl_it])
            elif flag_HOCL[hocl_it]['flag_Status'] == True:
                flag_HOCL[hocl_it]['flag_Status'] = True
                HOCL_to_topic.append(HOCL[hocl_it])
        else:
            if flag_HOCL[hocl_it]['flag_Status'] == True:
                flag_HOCL[hocl_it]['flag_Status'] = False
                HOCL_to_topic.append(HOCL[hocl_it])
            else:
                flag_HOCL[hocl_it]['flag_Status'] = False


    # --------------------------------------------------------------------------------------------
    #  STEP 3.2: Calculate the DI value that overcomes the 'Most population feels discomfort'
    #               category per point over the current real-time measurement
    # --------------------------------------------------------------------------------------------
    #
    # Choose cases where the criteria are fulfilled:
    #   DI >= list_of_values
    #   flag_location is True in previous iteration but in current iteration is altered to False

    print(" STEP 3.2: Calculate the DI value which exceeds a predefine category per area...")

    # list_of_values = ['No discomfort', 'Less than half population feels discomfort',
    #                   'More than half population feels discomfort', 'Most population feels discomfort',
    #                   'Heavy Discomfort', 'Very Strong Discomfort']

    # list_of_values = ['Heavy Discomfort', 'Very Strong Discomfort']

    # list_of_values = ['More than half population feels discomfort', 'Most population feels discomfort',
    #                   'Heavy Discomfort', 'Very Strong Discomfort']

    list_of_DI_Categories = [{'id':0, 'categ': 'No discomfort'},
                             {'id':1, 'categ': 'Less than half population feels discomfort'},
                             {'id':2, 'categ': 'More than half population feels discomfort'},
                             {'id':3, 'categ': 'Most population feels discomfort'},
                             {'id':4, 'categ': 'Heavy Discomfort'},
                             {'id':5, 'categ': 'Very Strong Discomfort'}
                             ]

    # Find the category id in which the category is equal with the CategThresh
    cat_id = [d['id'] for d in list_of_DI_Categories if CategThresh in d['categ']]

    # Formulate the list of values
    list_of_values = [ d['categ'] for d in list_of_DI_Categories if d['id'] >= cat_id[0] ]
    print(list_of_values)

    # list of dictionaries per point stores the results
    HWCrisis_Event = []

    for pnt in points:

        # Find from DSDI the DI_Category of the pnt
        cat_pnt = list(DSDI[ DSDI['Name'] == pnt['name'] ]['DI_Category'])

        temp_df = DSDI[DSDI['Name'] == pnt['name']]
        temp_df.reset_index(drop=True, inplace=True)

        for st in range(len(flag_location_status)):
            if flag_location_status[st]['name'] == pnt['name']:
                curr_st = flag_location_status[st]['flag_Status']
                pos_st = st

        # update status
        if cat_pnt[0] in list_of_values:

            flag_location_status[pos_st]['flag_Status'] = True

            item_cur = {'DateTime': temp_df['DateTime'].iloc[0], 'Name': temp_df['Name'].iloc[0],
                        'lat': temp_df['Lat'].iloc[0], 'long': temp_df['Long'].iloc[0],
                        'DI': temp_df['DI'].iloc[0], 'DI_Category': temp_df['DI_Category'].iloc[0],
                        'Color': temp_df['Color'].iloc[0]
                        }
            HWCrisis_Event.append(item_cur)

        else:
            if curr_st == True:
                flag_location_status[pos_st]['flag_Status'] = False

                item_cur = {'DateTime': temp_df['DateTime'].iloc[0], 'Name': temp_df['Name'].iloc[0],
                            'lat': temp_df['Lat'].iloc[0], 'long': temp_df['Long'].iloc[0],
                            'DI': 0, 'DI_Category': 'Null',
                            'Color': temp_df['Color'].iloc[0]
                            }

                HWCrisis_Event.append(item_cur)

            elif curr_st == False:
                print(" No update, still FALSE !!!")


    # End Timing Step 3
    end_step3 = time.time()
    time_duration_step.append(end_step3 - start_step3)

    # --------------------------------------------------------------------------------------------
    #  STEP 4: Creates the TOPIC_104_METRIC_REPORT
    # --------------------------------------------------------------------------------------------
    #
    # Topic 104 for Overall, Max, First

    print(" STEP 4: Creates the TOPIC_104_METRIC_REPORT... ")

    # Start Timing Step 4
    start_step4 = time.time()

    print(" \n Create topics... ")

    counter_topics = topic104HeatWave_Emerg(HOCL_to_topic, HWCrisis_Event, directory, points, center_points, valid_values)

    print("\n Total number of sending TOPIC104 is: ", counter_topics)

    # End Timing Step 4
    end_step4 = time.time()
    time_duration_step.append(end_step4 - start_step4)

    # -------------------------------------------------------------
    total_time = np.array(time_duration_step).sum()

    print("\n ****** EXECUTION TIME: **** ")

    Data_Acquisition = round(time_duration_step[0], 3)
    Data_Acquisition_min = round(time_duration_step[0] / 60.0, 3)
    print(" Time for Step 1. Data Acquisition from FMI: ", Data_Acquisition, " seconds")

    Parse_Data = round(time_duration_step[1], 3)
    Parse_Data_min = round(time_duration_step[1] / 60.0, 3)
    print(" Time for Step 2. Parse XLM and extract the data:", Parse_Data, " seconds")

    Culc_DI = round(time_duration_step[2], 3)
    Culc_DI_min = round(time_duration_step[2] / 60.0, 3)
    print(" Time for Step 3. Calculate DI & HOCL:", Culc_DI, " seconds")

    Calculations_Topics = round(time_duration_step[3], 3)
    Calculations_Topics_min = round(time_duration_step[3] / 60.0, 3)
    print(" Time for Step 4. Create & forward Topics:", Calculations_Topics, " seconds")

    Full_Time = round(total_time / 60.0, 3)
    Full_Time_secs = round(total_time, 3)
    print(" Total Execution Time: ", Full_Time, " minutes")

    # LIST OF TIMES
    Timers_lists = [['Data Acquisition', Data_Acquisition, Data_Acquisition_min],
                    ['Parse the Data', Parse_Data, Parse_Data_min],
                    ['Culculation of DI', Culc_DI, Culc_DI_min],
                    ['Topics Culculation ', Calculations_Topics, Calculations_Topics_min],
                    ['Total Time', Full_Time_secs, Full_Time]
                    ]

    Timers = pd.DataFrame(Timers_lists, columns=['Timer', 'Seconds', 'Minutes'])
    Time_csv = directory + "/" + "Evaluation_Time.csv"
    Timers.to_csv(Time_csv, encoding='utf-8', index=False)

    print("**** END OF PROGRAM ****")

    return flag_location_status
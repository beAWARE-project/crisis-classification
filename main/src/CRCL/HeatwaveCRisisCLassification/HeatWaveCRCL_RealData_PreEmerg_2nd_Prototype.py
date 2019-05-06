#!/usr/bin/env python3.6
#
# Created Date: 03/05/2019
# Modified Date:
#
# -------------------------------- 2nd PROTOTYPE VERSION AUTORUN -------------------------------------------------
#
# Implements the Early Warning Alert Algorithm of Heatwave Crisis Classification module
# based on the forecasting weather data from FMI. It calculates the Disconfort Index.
# Also, it calculates the Heatwave Overall Crisis Level (HOCL).
#
#----------------------------------------------------------------------------------------------------------
# Inputs: a) FMI weather forecasts from a region defined by a list of points in the
#               Thessaloniki region
#
# Outputs: TOP104_METRIC_REPORT which contains the ....
#
#   Early Warning Alert Algorithm from Crisis Classification (based on FMI data)
#----------------------------------------------------------------------------------------------------------
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
from CRCL.HeatwaveCRisisCLassification.parse_XML_dict import parse_XML_to_dict_querylist

import urllib.request
import xmltodict
import numpy as np

# from geopy.geocoders import Nominatim


def CrisisClassificationHeatwave_RealData_PreEmerg_v2(iteration, flag_location_status, flag_HOCL, CategThresh):

    ver = 'PreEmerg_RealData_2nd_PROTOTYPE_AUTORUN'

    # Create a directory to store the output files and TOPICS
    current_dirs_parent = os.getcwd()
    root_path_dir = current_dirs_parent + "/" + "CRCL/HeatwaveCRisisCLassification" + "/"
    now = datetime.now()

    # directory = root_path_dir + "TOPICS" + "_" + ver + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day)
    folder_name = "TOPICS_HW_Pre_EMERG_RealData_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + "/"
    directory = root_path_dir + folder_name + "TOPICS" + "_" + ver + "_" + str(iteration) + "_" + now.replace(microsecond=0).isoformat()
    os.makedirs(directory, exist_ok=True)

    # Start Timing Step 1
    start_step1 = time.time()

    # Store the time steps
    time_duration_step = []

    #-----------------------------------------------------------
    # Set current date/time - Find Epoch Time

    utc_current_date_time = datetime.utcnow().replace(microsecond=0)
    utc_current_date = utc_current_date_time.replace(hour=0, minute=0, second=0)

    epoch_cur_date_time = int(utc_current_date_time.timestamp())
    epoch_cur_date = int(utc_current_date.timestamp())

    print("****** SET UTC TIME: \n")
    print(" utc_current_date_time=", utc_current_date_time, ", epochs: ", epoch_cur_date_time)
    print(" utc_current_date=", utc_current_date, ", epochs: ", epoch_cur_date)

    #--------------------------------------------------------------------------------------------------
    # STEP 1: Fetch forecasting data from the FMI OpenData Web Feature Service (WFS)
    #
    # Query to FMI to extract forecasting weather data from a set of points in Thessaloniki region
    #
    #   point 1 Euosmos {'lat':40.664281, 'long':22.898388},
    #   point 2 Pl. Aristotelous {'lat': 40.632903, 'long': 22.940400}
    #   point 3 Faliro {'lat':40.619788, 'long':22.957993}
    #   point 4 Konstantinopolitika {'lat':40.611343, 'long':22.992180}
    #   point 5 Thermi - Xortiatis {'lat':40.581375, 'long':23.097996}
    #   point 6 Aerodromio {'lat':40.514354, 'long':22.986642}
    #
    print("\n STEP 1: Fetch forecasting data from the FMI OpenData Web Feature Service (WFS)...")

    fmi_addr = 'http://data.fmi.fi/fmi-apikey/'
    my_api_key = 'f96cb70b-64d1-4bbc-9044-283f62a8c734'
    data_format = 'multipointcoverage' #'timevaluepair'

    points = [ {'name':'Euosmos', 'lat':40.664281, 'long':22.898388},
               {'name':'Aristotelous Sq.', 'lat':40.632903, 'long':22.940400},
               {'name':'Faliro', 'lat':40.619788, 'long':22.957993},
               {'name':'Konstantinopolitika', 'lat':40.611343, 'long':22.992180},
               {'name':'Thermi-Xortiatis', 'lat':40.581375, 'long':23.097996},
               {'name':'Airport','lat':40.514354, 'long':22.986642}
             ]

    parameters = ["Temperature", "Humidity"]

    time_interval = 54

    # Call function to create query list
    qrlist = extract_forecasts_latlng(fmi_addr, my_api_key, data_format, parameters, points, time_interval)

    # End Timing Step 1
    end_step1 = time.time()
    time_duration_step.append( end_step1 - start_step1 )

    #---------------------------------------------------------------------------------------
    # STEP 2: Call function to parse XLM and extract the data

    print(" STEP 2: Parse XLM and extract the data... ")

    # Start Timing Step 2
    start_step2 = time.time()

    # Real Data
    res = parse_XML_to_dict_querylist(qrlist, points, directory)

    # Store data frame to xlsx file
    #
    dfxls = pd.ExcelWriter(directory + "/" + "DataFrame_Results_FMI.xlsx")
    res.to_excel(dfxls, 'Sheet1', index=False)
    dfxls.save()

    # End Timing Step 2
    end_step2 = time.time()
    time_duration_step.append( end_step2 - start_step2 )

    #----------------------------------------------------------------------------------------------
    #  STEP 3: Calculates the Disconfort Index (DI) for heatwave based on Temperature and Humidity
    #----------------------------------------------------------------------------------------------
    #

    print(" STEP 3: Calculates the Discomfort Index (DI)...")

    # Start Timing Step 3
    start_step3 = time.time()

    # constant threshold for temperature variable (from papers ~ 14.5) *******************************************
    InitTemp = 14.5
    model_param = 0.55

    DSDI = Heatwave_Disconfort_Index(res, InitTemp, model_param)

    dfxls = pd.ExcelWriter(directory + "/" + "DSDI_Results.xlsx")
    DSDI.to_excel(dfxls,'Sheet1', index=False)
    dfxls.save()

    #--------------------------------------------------------------------------------------------
    #  STEP 3.1: Calculate the Heatwave Overall Crisis Level per day over the 6 points
    #--------------------------------------------------------------------------------------------
    #

    print(" STEP 3.1: Calculates the Heatwave Overall Crisis Level per day over the 6 areas...")

    unMeasNum = DSDI['Measurement_Number'].unique()
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
    center_points = [ round(avgln/N,5), round(avglat/N,5) ]

    # list of days
    days = []
    unique_dates = list(DSDI['DateTime'].unique())
    for i in range(len(unique_dates)):
        days.append(unique_dates[i].split('T')[0].split('-')[2])

    days = np.array(days)
    days = np.unique(days, axis=0)
    days = np.sort(days)

    # Instead the above code:
    # #   datetime.strptime(df['DataTime'].iloc[0], "%Y-%m-%dT%H:%M:%S")

    for d in range(len(days)):

        # Split DSDI per day
        ds = DataFrame()
        for i in range(DSDI.shape[0]):
            if DSDI['DateTime'].iloc[i].split('T')[0].split('-')[2] == days[d]:
                temp_row = pd.DataFrame( DSDI.iloc[i] ).transpose()

                ds = pd.concat([ds, temp_row], axis=0, ignore_index=True)

        event_date_time = ds['DateTime'].iloc[0]

        # Call the function Heatwave_Overall_Crisis_Level
        item_dict = Heatwave_Overall_Crisis_Level(ds)

        item_dict.update( {'DateTime' : event_date_time} )
        item_dict.update( {'Position' : center_points} )

        HOCL.append( item_dict )

    print("\n HOCL: ", HOCL)

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


    #--------------------------------------------------------------------------------------------
    #  STEP 3.2: Calculate the 1st DI value that overcomes the 'Most population feels discomfort'
    #               category per point over all days
    #            Calculate the maximum DI value per point
    #--------------------------------------------------------------------------------------------
    #
    # Choose cases where DI >= list_of_values
    #

    print(" STEP 3.2: Calculate the 1st DI value which exceeds a predefine category per area and the max DI per area over all days...")

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

    #list_of_values = ['Less than half population feels discomfort','More than half population feels discomfort', 'Most population feels discomfort',
    #                 'Heavy Discomfort', 'Very Strong Discomfort']

    #list_of_values = ['More than half population feels discomfort', 'Most population feels discomfort', 'Heavy Discomfort', 'Very Strong Discomfort']

    #list_of_values = ['Most population feels discomfort', 'Heavy Discomfort', 'Very Strong Discomfort']


    # list of dictionaries per point stores the results
    First_HWCrisis_Event = []
    First_RemainingHours = []
    Max_HWCrisis_Event = []
    Max_RemainingHours = []


    print("iteration = ", iteration, ".... utc current_date-time =", utc_current_date_time, ", epochs=", epoch_cur_date_time )

    for pnt in points:

           temp_df =  DSDI[ DSDI['Name'] == pnt['name'] ]
           temp_df.reset_index(drop = True, inplace=True)

           if temp_df.shape[0] > 0:

               # find minimum/maximum value of DI in the specific point and its DI Category
               maxDI = temp_df['DI'].max()
               pos_maxDI = temp_df['DI'].idxmax()

               cat_pnt_max = temp_df['DI_Category'].iloc[pos_maxDI]

               # Category of the 1st DI value which exceeds a predefine category
               # find 1st position after the current hour
               #
               heatwave_risk_ds = temp_df[ temp_df['DI_Category'].isin(list_of_values) ]

               # if there exist valid entries in the heatwave_risk_ds which are above threshold category
               if heatwave_risk_ds.shape[0] > 0:

                   # Get the Hours from the utc_current_date_time and calculates its epoch time
                   utc_current_date_hour = utc_current_date_time.replace(minute=0, second=0)
                   epoch_utc_current_date_hour = int(utc_current_date_hour.timestamp())

                   iso_cur_dt = utc_current_date_hour.isoformat()

                   heatwave_risk_ds = heatwave_risk_ds[ heatwave_risk_ds['DateTime'] >= iso_cur_dt ]
                   heatwave_risk_ds.reset_index(drop=True, inplace=True)

                   cat_pnt_1st = heatwave_risk_ds['DI_Category'].iloc[0]

                   for st in range(len(flag_location_status)):
                       if flag_location_status[st]['name'] == pnt['name']:

                           curr_st_1st = flag_location_status[st]['flag_Status_1st']
                           curr_st_max = flag_location_status[st]['flag_Status_max']
                           pos_st = st

                           # update status for the 1st DI value which exceeds a predefine category
                           if cat_pnt_1st in list_of_values:
                               flag_location_status[pos_st]['flag_Status_1st'] = True

                               item_1st = { 'DateTime': heatwave_risk_ds['DateTime'].iloc[0], 'Name': heatwave_risk_ds['Name'].iloc[0],
                                            'lat': heatwave_risk_ds['Lat'].iloc[0], 'long': heatwave_risk_ds['Long'].iloc[0],
                                            'DI': heatwave_risk_ds['DI'].iloc[0], 'DI_Category': heatwave_risk_ds['DI_Category'].iloc[0],
                                            'Color': heatwave_risk_ds['Color'].iloc[0]
                                          }

                               # Calculate the Remaining Hours
                               date_1st = datetime.strptime(heatwave_risk_ds['DateTime'].iloc[0], "%Y-%m-%dT%H:%M:%S")
                               epoch_date_1st = int(date_1st.timestamp())

                               diff_1st = ( epoch_date_1st - epoch_utc_current_date_hour )/3600
                               print("\n diff_1st = ", diff_1st, " --- DI_Category: ", item_1st['DI_Category'] )

                               # For the Pilot reasons

                               if diff_1st == 0 and item_1st['DI_Category'] != 'No discomfort':
                                   diff_1st = 0.01
                               elif item_1st['DI_Category'] == 'No discomfort':
                                   diff_1st = 0

                               item_rh_1st = {'DateTime': heatwave_risk_ds['DateTime'].iloc[0], 'Name': heatwave_risk_ds['Name'].iloc[0],
                                              'lat': heatwave_risk_ds['Lat'].iloc[0], 'long': heatwave_risk_ds['Long'].iloc[0],
                                              'Remaining Hours': diff_1st
                                             }

                               # Update list of dictionaries
                               First_HWCrisis_Event.append(item_1st)
                               First_RemainingHours.append(item_rh_1st)

                           # elif cat_pnt_1st[0] not in list_of_values:
                           else:
                              if curr_st_1st == True:
                                    flag_location_status[pos_st]['flag_Status_1st'] = False

                                    item_1st = {'DateTime': heatwave_risk_ds['DateTime'].iloc[0], 'Name': heatwave_risk_ds['Name'].iloc[0],
                                    'lat': heatwave_risk_ds['Lat'].iloc[0], 'long': heatwave_risk_ds['Long'].iloc[0],
                                    'DI': 0, 'DI_Category': 'Null',
                                    'Color': heatwave_risk_ds['Color'].iloc[0]
                                    }

                                    item_rh_1st = {'DateTime': heatwave_risk_ds['DateTime'].iloc[0],
                                                   'Name': heatwave_risk_ds['Name'].iloc[0],
                                                   'lat': heatwave_risk_ds['Lat'].iloc[0],
                                                   'long': heatwave_risk_ds['Long'].iloc[0],
                                                   'Remaining Hours': 0
                                                   }
                                    # Update list of dictionaries
                                    First_HWCrisis_Event.append(item_1st)
                                    First_RemainingHours.append(item_rh_1st)

                              elif curr_st_1st == False:
                                  print(" No update 1st, still FALSE !!!")

                           # update status for the max DI value which exceeds a predefine category
                           if cat_pnt_max in list_of_values:

                               flag_location_status[pos_st]['flag_Status_max'] = True

                               item_max = {'DateTime': temp_df['DateTime'].iloc[pos_maxDI],
                                           'Name': temp_df['Name'].iloc[pos_maxDI],
                                           'lat': temp_df['Lat'].iloc[pos_maxDI], 'long': temp_df['Long'].iloc[pos_maxDI],
                                           'DI': temp_df['DI'].iloc[pos_maxDI],
                                           'DI_Category': temp_df['DI_Category'].iloc[pos_maxDI],
                                           'Color': temp_df['Color'].iloc[pos_maxDI]}

                               # Calculate the Maximum of the Remaining Hours
                               date_max = datetime.strptime(temp_df['DateTime'].iloc[pos_maxDI], "%Y-%m-%dT%H:%M:%S")
                               epoch_date_max = int(date_max.timestamp())

                               diff_max = ( epoch_date_max - epoch_utc_current_date_hour )/3600
                               print( "\n diff_max= ", diff_max, ' --- DI_Category: ', item_max['DI_Category'])

                               # For the Pilot reasons
                               if diff_max == 0 and item_max['DI_Category'] != 'No discomfort':
                                   diff_max = 0.01
                               elif item_max['DI_Category'] == 'No discomfort':
                                   diff_max = 0

                               item_rh_max = {'DateTime': temp_df['DateTime'].iloc[pos_maxDI],
                                              'Name': temp_df['Name'].iloc[pos_maxDI],
                                              'lat': temp_df['Lat'].iloc[pos_maxDI], 'long': temp_df['Long'].iloc[pos_maxDI],
                                              'Remaining Hours': diff_max
                                             }

                               # Update list of dictionaries
                               Max_HWCrisis_Event.append(item_max)
                               Max_RemainingHours.append(item_rh_max)

                           # elif cat_pnt_max[0] not in list_of_values:
                           else:
                               if curr_st_max == True:
                                   flag_location_status[pos_st]['flag_Status_max'] = False

                                   item_max = {'DateTime': temp_df['DateTime'].iloc[pos_maxDI],
                                               'Name': temp_df['Name'].iloc[pos_maxDI],
                                               'lat': temp_df['Lat'].iloc[pos_maxDI], 'long': temp_df['Long'].iloc[pos_maxDI],
                                               'DI': temp_df['DI'].iloc[pos_maxDI],
                                               'DI_Category': temp_df['DI_Category'].iloc[pos_maxDI],
                                               'Color': temp_df['Color'].iloc[pos_maxDI]}

                                   item_rh_max = {'DateTime': temp_df['DateTime'].iloc[pos_maxDI],
                                                  'Name': temp_df['Name'].iloc[pos_maxDI],
                                                  'lat': temp_df['Lat'].iloc[pos_maxDI], 'long': temp_df['Long'].iloc[pos_maxDI],
                                                  'Remaining Hours': 0}

                                   # Update list of dictionaries
                                   Max_HWCrisis_Event.append(item_max)
                                   Max_RemainingHours.append(item_rh_max)

                               elif curr_st_max == False:
                                   print(" No update max, still FALSE !!!")

               # if there no exist valid entries in the heatwave_risk_ds which are above threshold category
               else:
                    print(" In the point ", pnt['name'], ' DI values are less than ', list_of_values[0])


    # End Timing Step 3
    end_step3 = time.time()
    time_duration_step.append( end_step3 - start_step3 )

    # --------------------------------------------------------------------------------------------
    #  STEP 4: Creates the TOPIC_104_METRIC_REPORT
    # --------------------------------------------------------------------------------------------
    #
    # Topic 104 for Overall, Max, First

    print(" STEP 4: Creates the TOPIC_104_METRIC_REPORT... ")

    # Start Timing Step 4
    start_step4 = time.time()

    print(" \n Create topics... ")

    counter_topics = topic104HeatWave(HOCL_to_topic, First_HWCrisis_Event, Max_HWCrisis_Event, First_RemainingHours, Max_RemainingHours,
                                      directory, points, center_points, valid_values)

    print("\n Total number of sending TOPIC104 is: ", counter_topics)

    # End Timing Step 4
    end_step4 = time.time()
    time_duration_step.append(end_step4 - start_step4)

    #-------------------------------------------------------------
    total_time = np.array(time_duration_step).sum()


    print("\n ****** EXECUTION TIME: **** ")

    Data_Acquisition = round(time_duration_step[0], 3)
    Data_Acquisition_min = round(time_duration_step[0] / 60.0, 3)
    print(" Time for Step 1. Data Acquisition from FMI: ", Data_Acquisition, " seconds")

    Parse_Data = round(time_duration_step[1], 3)
    Parse_Data_min = round(time_duration_step[1]/ 60.0, 3)
    print(" Time for Step 2. Parse XLM and extract the data:", Parse_Data, " seconds")

    Culc_DI = round(time_duration_step[2], 3)
    Culc_DI_min = round(time_duration_step[2]/ 60.0, 3)
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

    print("\n PROGRAM IS TERMINATED!!! ")

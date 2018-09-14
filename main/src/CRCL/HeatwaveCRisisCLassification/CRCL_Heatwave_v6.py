#!/usr/bin/env python3.6
#
# Created Date: 11/09/2018
# Modified Date: 12/09/2018
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
import urllib.request
import xmltodict
import numpy as np

from CRCL.HeatwaveCRisisCLassification.Topic104_Metric_Report import Top104_Metric_Report
from CRCL.HeatwaveCRisisCLassification.topic104HeatWave import topic104HeatWave
from CRCL.HeatwaveCRisisCLassification.Auxiliary_functions import Heatwave_Disconfort_Index, Heatwave_Overall_Crisis_Level
from CRCL.HeatwaveCRisisCLassification.Create_Queries_HeatwavePilot import extract_forecasts_latlng

from CRCL.HeatwaveCRisisCLassification.parse_XML_dict import parse_XML_to_dict_querylist


def CrisisClassificationHeatwave_PreEmerg():

    ver = 'Ver6_2nd_Period'

    # Create a directory to store the output files and TOPICS

    # Create a path
    current_dirs_parent = os.getcwd()
    root_path = current_dirs_parent + "/" + "CRCL/HeatwaveCRisisCLassification" + "/"

    #root_path = Path.cwd()
    now = datetime.now()
    directory = root_path + "TOPICS" + "_" + ver + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day)
    os.makedirs(directory, exist_ok=True)


    # Start Timing Step 1
    start_step1 = time.time()

    # Store the time steps
    time_duration_step = []

    #-----------------------------------------------------------------------------------
    # STEP 1: Fetch data from the FMI OpenData Web Feature Service (WFS)
    #
    # Query to FMI to extract data from a set of points in Thessaloniki region
    #
    #   point 1 Euosmos {'lat':40.664281, 'long':22.898388},
    #   point 2 Pl. Aristotelous {'lat': 40.632903, 'long': 22.940400}
    #   point 3 Faliro {'lat':40.619788, 'long':22.957993}
    #   point 4 Konstantinopolitika {'lat':40.611343, 'long':22.992180}
    #   point 5 Thermi - Xortiatis {'lat':40.581375, 'long':23.097996}
    #   point 6 Aerodromio {'lat':40.514354, 'long':22.986642}
    #

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
    # STEP 2:
    # Call function to parse XLM and extract the data

    # Start Timing Step 2
    start_step2 = time.time()

    #res = parse_XML_to_dict_querylist(qrlist, points, directory)
    # Store data frame to xlsx file

    # dfxls = pd.ExcelWriter(directory + "/" + "DataFrame_Results.xlsx")
    # res.to_excel(dfxls,'Sheet1', index=False)
    # dfxls.save()

    #PATCH for review, after review use STEP 2
    #*************************** read an allready xlsx for standard values patch ********************
    xls_name = root_path + 'DataFrame_FAKE.xlsx'
    result = pd.read_excel(xls_name)

    dt = result['DateTime']

    newdt = []
    start_dt = datetime.strptime(dt[0], "%Y-%m-%dT%H:%M:%S")
    diff_days = datetime.now().day - start_dt.day

    for i in range(len(dt)):
        temp_dt = datetime.strptime(dt[i], "%Y-%m-%dT%H:%M:%S")
        newdt.append((temp_dt + timedelta(diff_days)).isoformat())

    result = result.drop('DateTime', 1)

    result['DateTime'] = newdt

    # Store data frame to xlsx file
    dfxls = pd.ExcelWriter(directory + "/" + "DataFrame_FAKE_Results.xlsx")
    result.to_excel(dfxls,'Sheet1', index=False)
    dfxls.save()

    #******************************************************************************************

    # End Timing Step 2
    end_step2 = time.time()
    time_duration_step.append( end_step2 - start_step2 )

    #----------------------------------------------------------------------------------------------
    #  STEP 3: Calculates the Disconfort Index (DI) for heatwave based on Temperature and Humidity
    #----------------------------------------------------------------------------------------------
    #

    # Start Timing Step 3
    start_step3 = time.time()

    # constant threshold for temperature variable (from papers ~ 14.5)
    InitTemp = 25 #14.5
    model_param = 0.55

    #*************************************************************
    #For PATCH
    DSDI = Heatwave_Disconfort_Index(result, InitTemp, model_param)
    #******************************************************************

    # For Step 2
    #DSDI = Heatwave_Disconfort_Index(res, InitTemp, model_param)

    dfxls = pd.ExcelWriter(directory + "/" + "DSDI_Results.xlsx")
    DSDI.to_excel(dfxls,'Sheet1', index=False)
    dfxls.save()

    #--------------------------------------------------------------------------------------------
    #  STEP 3.1: Calculate the Heatwave Overall Crisis Level per day over the 6 points
    #--------------------------------------------------------------------------------------------
    #

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

    center_points = [ round(avglat/N,5), round(avgln/N,5) ]
    #print(center_points)

    # list of days
    days = []
    unique_dates = list(DSDI['DateTime'].unique())
    for i in range(len(unique_dates)):
        days.append(unique_dates[i].split('T')[0].split('-')[2])

    days = list(set(days))
    #print(days)

    # Instead the above code:
    #   datetime.strptime(df['DataTime'].iloc[0], "%Y-%m-%dT%H:%M:%S")

    for d in range(len(days)):

        # Split DSDI per day
        ds = DataFrame()
        for i in range(DSDI.shape[0]):
            if DSDI['DateTime'].iloc[i].split('T')[0].split('-')[2] == days[d] :
                temp_row = pd.DataFrame( DSDI.iloc[i] ).transpose()
            #    print(temp_row)
                ds = pd.concat([ds, temp_row], axis=0, ignore_index=True)

        event_date_time = ds['DateTime'].iloc[0]

        # Call the function Heatwave_Overall_Crisis_Level
        item_dict = Heatwave_Overall_Crisis_Level(ds)

        item_dict.update( {'DateTime' : event_date_time} )
        item_dict.update( {'Position' : center_points} )

        HOCL.append( item_dict )

    # print(HOCL)

    #--------------------------------------------------------------------------------------------
    #  STEP 3.2: Calculate the 1st DI value that overcomes the 'Most population feels discomfort'
    #               category per point over all days
    #            Calculate the maximum DI value per point
    #--------------------------------------------------------------------------------------------
    #
    # Choose cases where DI >= list_of_values
    #
    #list_of_values = ['No discomfort', 'Less than half population feels discomfort','More than half population feels discomfort','Most population feels discomfort', 'Heavy Discomfort', 'Very Strong Discomfort']

    list_of_values = ['Heavy Discomfort', 'Very Strong Discomfort']

    heatwave_risk_ds = DSDI[ DSDI['DI_Category'].isin(list_of_values) ]
    heatwave_risk_ds.reset_index(drop = True, inplace=True)

    # print(heatwave_risk_ds)

    # list of dictionaries per point stores the results
    First_HWCrisis_Event = []
    Max_HWCrisis_Event = []

    for pnt in points:

       temp_df =  heatwave_risk_ds[ heatwave_risk_ds['Name'] == pnt['name'] ]
       temp_df.reset_index(drop = True, inplace=True)

       if temp_df.shape[0] > 0:

           # find minimum/maximum value of DI in the specific point
           maxDI = temp_df['DI'].max()
           pos_maxDI = temp_df['DI'].idxmax()

           item_1st = { 'DateTime': temp_df['DateTime'].iloc[0], 'Name': temp_df['Name'],
                        'lat':temp_df['Lat'].iloc[0], 'long':temp_df['Long'].iloc[0],
                        'DI': temp_df['DI'].iloc[0], 'DI_Category': temp_df['DI_Category'].iloc[0],
                        'Color': temp_df['Color'].iloc[0]
                  }
           First_HWCrisis_Event.append(item_1st)

           item_max = { 'DateTime': temp_df['DateTime'].iloc[pos_maxDI], 'Name': temp_df['Name'],
                        'lat':temp_df['Lat'].iloc[pos_maxDI], 'long':temp_df['Long'].iloc[pos_maxDI],
                        'DI': temp_df['DI'].iloc[pos_maxDI], 'DI_Category': temp_df['DI_Category'].iloc[pos_maxDI],
                        'Color': temp_df['Color'].iloc[pos_maxDI] }
           Max_HWCrisis_Event.append(item_max)

       else:
           print(" In the point ", pnt['name'], ' DI values are less than Most population feels discomfort category ')

    # print(First_HWCrisis_Event)
    # print(Max_HWCrisis_Event)

    # End Timing Step 3
    end_step3 = time.time()
    time_duration_step.append( end_step3 - start_step3 )

    #--------------------------------------------------------------------------------------------
    #  STEP 4: Creates the TOPIC_104_METRIC_REPORT
    #--------------------------------------------------------------------------------------------
    #
    #Topic 104 for Overall, Max, First

    # Start Timing Step 4
    start_step4 = time.time()

    print(" \n Create topics... ")

    counter_topics = topic104HeatWave(HOCL,First_HWCrisis_Event,Max_HWCrisis_Event,directory,points,center_points)

    print("\n Total number of sending TOPIC104 is: ", counter_topics)

    # End Timing Step 4
    end_step4 = time.time()
    time_duration_step.append( end_step4 - start_step4 )

    #-------------------------------------------------------------
    total_time = np.array(time_duration_step).sum()

    print("\n ****** EXECUTION TIME: **** ")
    print(" Time for Step 1. Data Acquisition from FMI: ", round(time_duration_step[0],5), " seconds")
    print(" Time for Step 2. Parse XLM and extract the data:", round(time_duration_step[1],5), " seconds")
    print(" Time for Step 3. Calculate DI & HOCL:", round(time_duration_step[2],5), " seconds")
    print(" Time for Step 4. Create & forward Topics:", round(time_duration_step[3],5), " seconds")
    print(" Total Execution Time: ", round(total_time/60.0,5), " minutes")


    print("\n PROGRAM IS TERMINATED!!! " )

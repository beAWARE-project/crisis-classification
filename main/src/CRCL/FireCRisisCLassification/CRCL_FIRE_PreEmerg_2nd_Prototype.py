# Created Date: 03/05/2019
# Modified Date:
# Last Modified From:
#
# -------------------------------- 2nd PROTOTYPE VERSION ---------------------------------------------------
#
# Implements the Early Warning Alert Algorithm of Fire Crisis Classification module
# based on the forecasting weather data from FMI. It calculates the Fire Weather Index
# of Canadian Rating System.
# Also, it calculates the Fire Overall Crisis Level (PFRCL_Predicted Fire Crisis Level)
# based on FWI over the 9 days period
#
# ----------------------------------------------------------------------------------------------------------
# Inputs: a) ftp files from EFFIS
#
# Outputs: TOP104_METRIC_REPORT which contains the ....
#
#   Early Warning Alert Algorithm from Crisis Classification
# ----------------------------------------------------------------------------------------------------------
#

import json, time, re
import os, errno, sys
import glob, gzip, pickle, shutil, tempfile, re, tarfile
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import netCDF4

#from bus.bus_producer import BusProducer
#from bus.CRCL_service import CRCLService
from pathlib import Path
from datetime import datetime, timedelta
from collections import OrderedDict

from CRCL.FireCRisisCLassification.Topic104_Metric_Report import *
from CRCL.FireCRisisCLassification.Auxiliary_functions import *
from CRCL.FireCRisisCLassification.Create_Queries_FirePilot import *
from CRCL.FireCRisisCLassification.parse_XML_dict import *
from CRCL.FireCRisisCLassification.topic104Fire import *

from scipy.interpolate import griddata
from scipy.interpolate import Rbf
from scipy import interpolate
from netCDF4 import Dataset, num2date


def CrisisClassificationFire_PreEmerg_v2():

    ver = 'PreEmerg_2nd_PROTOTYPE'

    # Create a directory to store the output files and TOPICS
    current_dirs_parent = os.getcwd()
    root_path_dir = current_dirs_parent + "/" + "CRCL/FireCRisisCLassification" + "/"

    now = datetime.now()
    folder_name = "Files_FIRE_preEMERG_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day) + "/"

    directory = root_path_dir + folder_name + "TOPICS" + "_" + ver + "_" + now.replace(microsecond=0).isoformat()
    os.makedirs(directory, exist_ok=True)

    #####
    directory_forNC = root_path_dir + folder_name
    os.makedirs(directory_forNC, exist_ok=True)
    #####


    # Create a directory to store the output files and TOPICS
    # root_path = Path.cwd()
    # now = datetime.now()
    # directory = "TOPICS_JUPYTER_" + ver + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day)
    # os.makedirs(directory, exist_ok=True)
    # Start Timing Step 1
    # start_step1 = time.time()


    # # Store the time steps
    # time_duration_step = []

    print("\n STEP 1: Fetch data from the ftp files from EFFIS \n")

    # -----------------------------------------------------------------------------------
    # STEP 1: Fetch data from the ftp files from EFFIS
    #
    #
    # parameters for the get_ftp
    url = 'dissemination.ecmwf.int'
    Username = 'firepub'
    Password = 'firepub2018'

    points = [{'Name': 'Sueca', 'lat': 39.253509, 'long': -0.310381},
              {'Name': 'Sollana', 'lat': 39.303946, 'long': -0.379010},
              {'Name': 'Silla1', 'lat': 39.340604, 'long': -0.395129},
              {'Name': 'Silla2', 'lat': 39.364153, 'long': -0.371332},
              {'Name': 'Catarroja', 'lat': 39.371835, 'long': -0.350579},
              {'Name': 'El Saler', 'lat': 39.386909, 'long': -0.331496},
              {'Name': 'Les Gavines', 'lat': 39.355179, 'long': -0.320472}]

    # Center of the points of interest
    N = float(len(points))
    avglat = 0
    avgln = 0
    for p in points:
        avglat = avglat + p['lat']
        avgln = avgln + p['long']

    center_points = [round(avglat / N, 5), round(avgln / N, 5)]

    # file_type = '*fwi.nc.gz'
    fieldNames = ['fwi']  # ['danger_risk', 'fwi']
    # fieldNames = ['danger_risk','bui','ffmc','dc', 'dmc','isi', 'dsr', 'fwi']

    ncep_data = dict()
    ncep_data['date_time'] = list()

    # get the file name we dll from the ftp
    ftp_dict = get_ftp(url, Username, Password, directory_forNC)
    file = ftp_dict['PATH_flname']

    # file_type = ftp_dict['Filename']
    #path = ftp_dict['PATH_flname']

    iter = [0] * len(fieldNames)
    name = fieldNames[0]

    # Open the file we downloaded and take its vals
    dataFile = open_netcdf(file)
    lons, lats, date_time, fieldvalues = get_nc_file_contents(dataFile, name)

    for dts in range(1, len(date_time)):

        # Create data frame for all the points and FWI estimations
        FWI = pd.DataFrame()
        print('For the Date: ' + str(dts))

        for pnt in range(len(points)):

            tempFWI = pd.DataFrame()
            fwi_date = []
            # fwi_val = pd.DataFrame(columns=['FWI_lin', 'FWI_near','FWI_cubic' ,'FWI_max'])
            fwi_val = pd.DataFrame(
                columns=['FWI_lin', 'FWI_near', 'FWI_cubic', 'FWI_max', 'FWI_min', 'FWI_std', 'FWI_mean'])
            print('Working on: ' + file)
            print('For the point: ' + str(pnt))

            # for file in sorted(glob.glob(os.path.join(path, file_type))):

            # Interpolate the FWI (output: linear, nearest, cubic)
            LAT_V = points[pnt]['lat']
            LON_V = points[pnt]['long']

            # no_pnt: number of points to use for interpolation around to a point of interest per coordinate
            no_pnt = 3
            z_int = calc_Index(dts, lons, lats, fieldvalues, LAT_V, LON_V, no_pnt)

            # z_int = calc_Index(lons, lats, fieldvalues, LAT_V, LON_V)

            temp_z = pd.DataFrame(z_int).transpose()
            print(temp_z)
            # remove cubic intepolation if exists
            if temp_z.shape[0] > 3:
                temp_z.drop(temp_z.columns[2], axis=1)

            temp_z.columns = fwi_val.columns
            # print(date_time)

            # The date that we are in progress with isoformat
            fwi_date.append(date_time[dts].isoformat())
            #     for i in range(1,10):
            #         fwi_date.append(date_time[i].isoformat())

            fwi_val = pd.concat([fwi_val, temp_z], axis=0, ignore_index=True)

            # create the file structure
            timestamp = file.split('_')[2]
            my_file = Path('./' + name + ".txt")
            if my_file.is_file() and iter[fieldNames.index(name)] != 0:
                thefile = open(my_file, "a")
            else:
                thefile = open(my_file, "w")

            thefile.write(timestamp + ";" + str(temp_z) + '\n')
            thefile.close()
            iter[fieldNames.index(name)] += 1

            # Step 1.1: Create the whole dataframe FWI
            # Create the whole dataframe FWI
            num_rows = fwi_val.shape[0]
            tempFWI['Name'] = pd.concat([pd.DataFrame([points[pnt]['Name']], columns=['Name']) for i in range(num_rows)],
                                        ignore_index=True)

            tempFWI['Lat'] = [points[pnt]['lat']] * num_rows
            tempFWI['Long'] = [points[pnt]['long']] * num_rows
            tempFWI['Date'] = fwi_date
            tempFWI = pd.concat([tempFWI, fwi_val], axis=1, ignore_index=True)

            #     print("\n *************** iter = ", pnt)
            #     print("tempFWI ", tempFWI.shape ,"\n")
            #     print(tempFWI)

            FWI = pd.concat([FWI, tempFWI], axis=0, ignore_index=True)

        # Give column names to DataFrame
        FWI.columns = ['Name', 'Lat', 'Long', 'Date', 'FWI_lin', 'FWI_near', 'FWI_cubic', 'FWI_max', 'FWI_min', 'FWI_std',
                       'FWI_mean']
        # print(FWI)

        # Store FWI dataframe to excel file
        # FWM[Date] is always same in each DF  so i use first [0]
        fwixls = pd.ExcelWriter(directory + "/" + "FWI_date_" + FWI['Date'][0] + ".xlsx")
        FWI.to_excel(fwixls, 'Sheet1', index=False)
        fwixls.save()

        # # End Timing Step 1
        # end_step1 = time.time()
        # time_duration_step.append( end_step1 - start_step1 )

        print("\n ------------------------------------ \n")
        print(" STEP 2: Estimate the Fire Danger, Date no:" + str(dts))
        # ----------------------------------------------------------------------------------------------
        # STEP 2: Estimate the Fire Danger based on the WFI forecasting values per day for 9 days ahead
        #           for each point.
        #
        # choose interpolation method:
        # - 'lin' for linear
        # - 'near' for nearest neighbor
        # - 'max' for maximum FWI value for all the grid points which are nearby the point of interest
        # - 'cubic' for cubic interpolation method
        #
        # interp_method = 'near'
        # interp_method = 'max'

        # # Start Timing Step 2
        # start_step2 = time.time()

        interp_method = 'cubic'

        df = Forest_Fire_Weather_Index(FWI, interp_method)

        # Store df dataframe to excel file
        dfxls = pd.ExcelWriter(directory + "/" + "DataFrame_FWI_DangerIndex_no:" + str(dts) + ".xlsx")
        df.to_excel(dfxls, 'Sheet1', index=False)
        dfxls.save()
        print(df['Fire_Danger'])

        # Keep only the WFI values from current date/time
        cur_date = datetime.utcnow().replace(microsecond=0).isoformat().split("T")
        df = df[df['Date'] >= cur_date[0]].reset_index()

        # # End Timing Step 2
        # end_step2 = time.time()
        # time_duration_step.append( end_step2 - start_step2 )

        print("\n ------------------------------------ \n")
        print(" STEP 3: Calculate Fire Overall Crisis Level no:" + str(dts))
        # ----------------------------------------------------------------------------------------------
        # STEP 3: Calculate Fire Overall Crisis Level per day over all the points
        #

        # # Start Timing Step 3
        # start_step3 = time.time()

        unq_dates = df['Date'].unique()

        # FOCL per day
        FOCL_list = []

        categories = ['Very Low Danger', 'Low Danger', 'Moderate Danger', 'High Danger', 'Very High Danger',
                      'Extreme Danger']

        for iter_date in range(len(unq_dates)):

            CountFWI = []
            ds_date = df[df['Date'] == unq_dates[iter_date]]

            for i in range(len(categories)):
                temp_df = pd.DataFrame(ds_date[0:df.shape[0]])
                temp_df = pd.DataFrame(temp_df[temp_df['Fire_Danger'] == categories[i]])
                item = {'Note': categories[i], 'Count': temp_df.shape[0]}
                CountFWI.append(item)

            focl = Fire_Overall_Crisis_Level(CountFWI)
            focl.update({'Date': unq_dates[iter_date], 'Position': center_points})

            FOCL_list.append(focl)

        # Find the 1st observation which its FWI value exceeds Moderate category per point
        # and also find the maximum FWI per point at the 9 days period
        df_1st = pd.DataFrame()
        df_max = pd.DataFrame()

        # including Moderate
        # interest_categ = categories[2:]
        interest_categ = categories[0:]

        colname = 'FWI' + "_" + interp_method

        for pts in range(len(points)):

            temp_df1st = df[df['Fire_Danger'].isin(interest_categ)]
            temp_df1st = temp_df1st[temp_df1st['Name'] == points[pts]['Name']].reset_index()

            # Update DataFrames
            if temp_df1st.shape[0] > 0:
                df_1st = pd.concat([df_1st, pd.DataFrame(temp_df1st.iloc[0]).transpose()], ignore_index=True)

                # find maximum FWI value
                posmax = temp_df1st[colname].idxmax()
                df_max = pd.concat([df_max, pd.DataFrame(temp_df1st.iloc[posmax]).transpose()], ignore_index=True)

            else:
                print("No FWI values exceed Moderate Fire Danger for ", points[pts]['Name'])

        # # End Timing Step 3
        # end_step3 = time.time()
        # time_duration_step.append( end_step3 - start_step3 )

        print("\n ------------------------------------ \n")
        print(" STEP 4: Call function to create TOPICS")

        # ------------------------------------------------------------------------------------------
        # STEP 4: Call function to create TOPICS

        # # Start Timing Step 4
        # start_step4 = time.time()

        noTop104 = topic104Fire(directory, df_1st, df_max, FOCL_list, categories, interp_method, dts)

        print("\n Total number of TOP104 is:", noTop104)

    # # End Timing Step 4
    # end_step4 = time.time()
    # time_duration_step.append( end_step4 - start_step4 )

    # --------------------------------------------------------------------------------------------------
    # total_time = np.array(time_duration_step).sum()


    # print("\n ****** EXECUTION TIME: **** ")
    # print(" Time for Step 1. Data Acquisition: ", round(time_duration_step[0],3), " seconds")
    # print(" Time for Step 2. Estimate Fire Danger:", round(time_duration_step[1],3), " seconds")
    # print(" Time for Step 3. Estimate FOCL:", round(time_duration_step[2],3), " seconds")
    # print(" Time for Step 4. Create & forward Topics:", round(time_duration_step[3],3), " seconds")
    # print(" Total Execution Time: ", round(total_time/60.0,3), " minutes")
    print("\n PROGRAM IS TERMINATED!!! ")


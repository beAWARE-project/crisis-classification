# Created Date: 12/09/2018
# Modified Date:
#
# Implements the Early Warning Alert Algorithm of Fire Crisis Classification module
# based on the forecasting weather data from FMI. It calculates the Fire Weather Index
# of Canadian Rating System.
# Also, it calculates the Fire Overall Crisis Level (PFRCL_Predicted Fire Crisis Level)
# based on FWI over the 9 days period
#
#----------------------------------------------------------------------------------------------------------
# Inputs: a) ftp files from EFFIS
#
# Outputs: TOP104_METRIC_REPORT which contains the ....
#
#   Early Warning Alert Algorithm from Crisis Classification
#----------------------------------------------------------------------------------------------------------
#
import json, time, re
import os, errno, sys
import glob, gzip, pickle, shutil, tempfile, re, tarfile
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from bus.bus_producer import BusProducer

from pathlib import Path
from datetime import datetime, timedelta
from collections import OrderedDict
from CRCL.FireCRisisCLassification.Topic104_Metric_Report import Top104_Metric_Report
from CRCL.FireCRisisCLassification.topic104Fire import topic104Fire
from CRCL.FireCRisisCLassification.Auxiliary_functions import Forest_Fire_Weather_Index, Fire_Overall_Crisis_Level
from CRCL.FireCRisisCLassification.Auxiliary_functions import open_netcdf, get_nc_file_contents, geo_idx, get_ftp, calc_Index
from CRCL.FireCRisisCLassification.Create_Queries_FirePilot import extract_forecasts_grid, extract_gribs
from CRCL.FireCRisisCLassification.parse_XML_dict import parse_XML_dict
from ftplib import FTP
from scipy.interpolate import griddata
from scipy.interpolate import Rbf
from scipy import interpolate
from netCDF4 import Dataset, num2date



def CrisisClassificationFire_PreEmerg():

    #-----------------------------------------------------------------
    ver = 'Ver11_2nd_Period'

    # Create a directory to store the output files and TOPICS
    #root_path = Path.cwd()

    # Create a path
    current_dirs_parent = os.getcwd()
    root_path = current_dirs_parent + "/" + "CRCL/FireCRisisCLassification" + "/"

    now = datetime.now()
    directory = root_path + "TOPICS" + ver + "_" + str(now.year) + "_" + str(now.month) + "_" + str(now.day)
    os.makedirs(directory, exist_ok=True)

    # Start Timing Step 1
    start_step1 = time.time()

    # Store the time steps
    time_duration_step = []

    print("\n STEP 1: Fetch data from the ftp files from EFFIS \n")

    #-----------------------------------------------------------------------------------
    # STEP 1: Fetch data from the ftp files from EFFIS
    #
    #
    #parameters for the get_ftp
    url='dissemination.ecmwf.int'
    Username='fire'
    Password='fire2017'

    # Points of Interest
    points = [{'Name': 'Sueca', 'lat': 39.253509, 'long': -0.310381},
              {'Name': 'Sollana', 'lat': 39.303946, 'long': -0.379010},
              {'Name': 'Silla1', 'lat': 39.340604, 'long': -0.395129},
              {'Name': 'Silla2', 'lat': 39.364153, 'long': -0.371332},
              {'Name': 'Catarroja', 'lat': 39.371835, 'long': -0.350579},
              {'Name':'El Saler', 'lat':39.355179, 'long':-0.320472},
              {'Name':'Les Gavines', 'lat':39.386909, 'long': -0.331496}
              ]

    # Center of the points of interest
    N = float(len(points))
    avglat = 0
    avgln = 0
    for p in points:
        avglat = avglat + p['lat']
        avgln = avgln + p['long']

    center_points = [ round(avglat/N,5), round(avgln/N,5) ]

    file_type = '*fwi.nc.gz'
    fieldNames = ['fwi']  # ['danger_risk', 'fwi']
    # fieldNames = ['danger_risk','bui','ffmc','dc', 'dmc','isi', 'dsr', 'fwi']

    ncep_data = dict()
    ncep_data['date_time'] = list()

    # get the file name we dll from the ftp
    ftp_dict = get_ftp(url, Username, Password)

    # PATH variable include / at the end...
    path = ftp_dict['PATH'] + str(ftp_dict['Date']) + '_fwi/fc/'

    iter = [0] * len(fieldNames)

    # Create data frame for all the points and FWI estimations
    FWI = pd.DataFrame()

    for pnt in range(len(points)):

        datetime_x = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        days_x = 1
        tempFWI = pd.DataFrame()
        fwi_date = []
        fwi_val = pd.DataFrame(columns=['FWI_lin', 'FWI_near', 'FWI_cubic', 'FWI_max', 'FWI_min', 'FWI_std', 'FWI_mean'])

        for file in sorted(glob.glob(os.path.join(path, file_type))):
            print('Working on: ' + file)
            namelist = [name for name in fieldNames if re.search(name, file)]
            name = namelist[0]

            dataFile = open_netcdf(file)
            lons, lats, date_time, fieldvalues = get_nc_file_contents(dataFile, name)

            # Interpolate the FWI (output: linear, nearest, cubic)
            LAT_V = points[pnt]['lat']
            LON_V = points[pnt]['long']

            # no_pnt: number of points to use for interpolation around to a point of interest per coordinate
            no_pnt = 3
            z_int = calc_Index(lons, lats, fieldvalues, LAT_V, LON_V, no_pnt)

            temp_z = pd.DataFrame(z_int).transpose()
            # remove cubic intepolation if exists
            if temp_z.shape[0] > 3:
                temp_z.drop(temp_z.columns[2], axis=1)

            temp_z.columns = fwi_val.columns

            #fwi_date.append(date_time[0].isoformat())

            fwi_date.append(datetime_x.isoformat())
            datetime_x = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=days_x)
            days_x += 1

            fwi_val = pd.concat([fwi_val, temp_z], axis=0, ignore_index=True)

            # create the file structure
            # timestamp = file.split('_')[2]
            # my_file = Path('./' + name + ".txt")
            # if my_file.is_file() and iter[fieldNames.index(name)] != 0:
            #     thefile = open(my_file, "a")
            # else:
            #     thefile = open(my_file, "w")
            #
            # thefile.write(timestamp + ";" + str(temp_z) + '\n')
            # thefile.close()

            iter[fieldNames.index(name)] += 1

        #-----------------------------------------------------
        # Step 1.1: Create the whole dataframe FWI
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
    FWI.columns=['Name', 'Lat', 'Long', 'Date', 'FWI_lin', 'FWI_near','FWI_cubic','FWI_max', 'FWI_min', 'FWI_std', 'FWI_mean']
    #print(FWI)


    # Store FWI dataframe to excel file
    fwixls = pd.ExcelWriter(directory + "/" + "FWI_data.xlsx")
    FWI.to_excel(fwixls, 'Sheet1', index=False)
    fwixls.save()

    # End Timing Step 1
    end_step1 = time.time()
    time_duration_step.append( end_step1 - start_step1 )


    print("\n ------------------------------------ \n")
    print(" STEP 2: Estimate the Fire Danger ")
    #----------------------------------------------------------------------------------------------
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

    # Start Timing Step 2
    start_step2 = time.time()

    interp_method = 'cubic'

    df = Forest_Fire_Weather_Index(FWI, interp_method)

    # Store df dataframe to excel file
    dfxls = pd.ExcelWriter(directory + "/" + "DataFrame_FWI_DangerIndex.xlsx")
    df.to_excel(dfxls, 'Sheet1', index=False)
    dfxls.save()

    # Keep only the WFI values from current date/time
    cur_date = datetime.utcnow().replace(microsecond=0).isoformat().split("T")
    df = df[df['Date'] >= cur_date[0]].reset_index()

    # End Timing Step 2
    end_step2 = time.time()
    time_duration_step.append( end_step2 - start_step2 )

    print("\n ------------------------------------ \n")
    print(" STEP 3: Calculate Fire Overall Crisis Level ")
    #----------------------------------------------------------------------------------------------
    # STEP 3: Calculate Fire Overall Crisis Level per day over all the points
    #

    # Start Timing Step 3
    start_step3 = time.time()

    unq_dates = df['Date'].unique()

    # FOCL per day
    FOCL_list = []

    categories = ['Very Low Danger', 'Low Danger', 'Moderate Danger', 'High Danger', 'Very High Danger', 'Extreme Danger']

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

    # including High
    interest_categ = categories[3:]

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
            print("No WFI values exceed Moderate Fire Danger for ", points[pts]['Name'])


    # End Timing Step 3
    end_step3 = time.time()
    time_duration_step.append( end_step3 - start_step3 )

    print("\n ------------------------------------ \n")
    print(" STEP 4: Call function to create TOPICS" )

    #------------------------------------------------------------------------------------------
    # STEP 4: Call function to create TOPICS

    # Start Timing Step 4
    start_step4 = time.time()

    noTop104 = topic104Fire(directory, df_1st, df_max, FOCL_list, categories, interp_method)

    print("\n Total number of TOP104 is:", noTop104)


    # End Timing Step 4
    end_step4 = time.time()
    time_duration_step.append( end_step4 - start_step4 )

    #--------------------------------------------------------------------------------------------------
    total_time = np.array(time_duration_step).sum()


    print("\n ****** EXECUTION TIME: **** ")
    print(" Time for Step 1. Data Acquisition: ", round(time_duration_step[0],3), " seconds")
    print(" Time for Step 2. Estimate Fire Danger:", round(time_duration_step[1],3), " seconds")
    print(" Time for Step 3. Estimate FOCL:", round(time_duration_step[2],3), " seconds")
    print(" Time for Step 4. Create & forward Topics:", round(time_duration_step[3],3), " seconds")
    print(" Total Execution Time: ", round(total_time/60.0,3), " minutes")


    print("\n PROGRAM IS TERMINATED!!! ")


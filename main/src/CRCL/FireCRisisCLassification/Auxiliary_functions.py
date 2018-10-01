import pandas as pd
import numpy as np
import os, errno
import glob, gzip, pickle, shutil, tempfile, re, tarfile
from ftplib import FTP
import time
from netCDF4 import Dataset, num2date
from scipy.interpolate import griddata
from scipy.interpolate import Rbf
from scipy import interpolate


#-------------------------------------------------------------------------------
def Simple_Fire_Danger_Index(dset, Uo):

    # Take Temperature values for the i-th grid (Measurement_Number)
    temp_ds = DataFrame(dset.loc[dset['Name'] == '2 metre temperature'])
    # Convert temperature values in Kelvin to Celcius
    temp_col = temp_ds['Value'] - 273.15
    temp_ds['Temp_C'] = temp_col

    # new data frame
    df = DataFrame(temp_ds.loc[:, ['Measurement_Number', 'DateTime', 'Lat', 'Long', 'Temp_C']])

    # Take the humidity column from dset and add it to df
    humid_ds = DataFrame(dset.loc[dset['Name'] == '2 metre relative humidity'])
    hc = humid_ds['Value'].reset_index(drop=True)
    df['Rel_Humidity'] = hc

    # Take the wind speec column from dset and add it to df
    wind_sp_ds = DataFrame(dset.loc[dset['Name'] == '10 metre wind speed'])
    wsp = wind_sp_ds['Value'].reset_index(drop=True) * 3.6
    df['Wind_Speed_Km/h'] = wsp

    # Find maximum value of Wind Speed per Date/Time (or Measurement Number)
    # and calculate Simple Fire Index per hour

    SFDI = []
    unique_meas = df['Measurement_Number'].unique()

    for i in range(len(unique_meas)):
        temp_df = DataFrame(df.loc[df['Measurement_Number'] == unique_meas[i]])

        # maximum Wind Speed
        U = []
        for j in range(len(temp_df['Wind_Speed_Km/h'])):
            if Uo > temp_df['Wind_Speed_Km/h'].iloc[j]:
                U.append(Uo)
            else:
                U.append(temp_df['Wind_Speed_Km/h'].iloc[j])

        # calculate FMI and SFI per measurement_number
        FMI = 10 - 0.25 * (temp_df['Temp_C'] - temp_df['Rel_Humidity'])

        F = list( round(U/FMI, 3) )
        SFDI += F

    # Add SFDI to df
    df['SFDI'] = SFDI

    # Calculate the Fire Risk based on the index SFDI
    Fire_Risk = []
    Color = []
    for i in range(df.shape[0]):
        if df['SFDI'].iloc[i] >= 0 and df['SFDI'].iloc[i] < 0.7:
            Fire_Risk.append('Low')
            Color.append('#00FF00') # green (lime)
        elif df['SFDI'].iloc[i] >= 0.7 and df['SFDI'].iloc[i] < 1.5:
            Fire_Risk.append('Moderate')
            Color.append('#4682B4')  # steelblue
        elif df['SFDI'].iloc[i] >= 1.5 and df['SFDI'].iloc[i] < 2.7:
            Fire_Risk.append('High')
            Color.append('#FF8000') # orange
        elif df['SFDI'].iloc[i] >= 2.7 and df['SFDI'].iloc[i] < 6.1:
            Fire_Risk.append('Very High')
            Color.append('#FF0000') # red
        else:
            Fire_Risk.append('Extreme')
            Color.append('#8B0000')  # red darked

    df['Fire_Risk'] = Fire_Risk
    df['color'] = Color

    return(df)

#-------------------------------------------------------------------------------
#   OBSOLETE VERSION UP TO VER 7
#
# def Forest_Fire_Weather_Index(ds):
#
#     # Calculate the danger of fire based on Canadian Forest Fire Weather Index
#     ffwi = []
#     for i in range( ds.shape[0] ):
#         fire_hazard = ds['FWI'].iloc[i]
#
#         if fire_hazard < 2:
#             note = 'Very Low Danger'
#             color = '#00FF00' # green (lime)
#         elif fire_hazard >=2  and fire_hazard <6 :
#             note = 'Low Danger'
#             color = '#FFFF00' # yellow
#         elif fire_hazard >= 6 and fire_hazard < 13:
#             note = 'Moderate Danger'
#             color = '#FFC125'  # goldenrod 1
#         elif fire_hazard >= 13 and fire_hazard < 26:
#             note = 'High Danger'
#             color = '#FF8000' # orange
#         elif fire_hazard >= 26 and fire_hazard < 48:
#             note = 'Very High Danger'
#             color = '#FF0000' # red
#         else:  # exceed 48
#             note = 'Extreme Danger'
#             color = '#8B0000'  # red darked
#
#         item = {'Date': ds['Date'].iloc[i], 'FWI': fire_hazard, 'note': note, 'color': color}
#         ffwi.append(item)
#
#     result_ffwi = pd.DataFrame(ffwi)
#
#     return(result_ffwi)

#-------------------------------------------------------------------------------
# NEW VERSION VER 8 AND AFTER
#
def Forest_Fire_Weather_Index(ds, interp_method):
    fwi_col_name = 'FWI_' + interp_method

    # Calculate the danger of fire based on Canadian Forest Fire Weather Index
    ffwi = []

    for i in range(ds.shape[0]):
        fire_hazard = ds.loc[i, fwi_col_name]

        if fire_hazard < 2:
            note = 'Very Low Danger'
            color = '#00FF00'  # green (lime)
        elif fire_hazard >= 2 and fire_hazard < 6:
            note = 'Low Danger'
            color = '#FFFF00'  # yellow
        elif fire_hazard >= 6 and fire_hazard < 13:
            note = 'Moderate Danger'
            color = '#FFC125'  # goldenrod 1
        elif fire_hazard >= 13 and fire_hazard < 26:
            note = 'High Danger'
            color = '#FF8000'  # orange
        elif fire_hazard >= 26 and fire_hazard < 48:
            note = 'Very High Danger'
            color = '#FF0000'  # red
        else:  # exceed 48
            note = 'Extreme Danger'
            color = '#8B0000'  # red darked

        item = {'Date': ds['Date'].iloc[i], 'FWI': fire_hazard, 'note': note, 'color': color}
        ffwi.append(item)

    temp_df = pd.DataFrame(ffwi)

    result_ffwi = pd.concat([ds, temp_df['note'], temp_df['color']], axis=1, ignore_index=True)

    # NEW CODE Ver 9
    # Create columns names of the new data.frame result_ffwi
    # ds_cols = ds.columns.columns.values.tolist()
    ds_cols = list(ds)
    ds_cols.append('Fire_Danger')
    ds_cols.append('Color')
    result_ffwi.columns = ds_cols

    #  Obsolete code up to version 8
    # print(result_ffwi.shape)
    # if result_ffwi.shape[1] == 10:
    #     result_ffwi.columns = ['index', 'Name', 'Lat', 'Long', 'Date', 'FWI_lin', 'FWI_near', 'FWI_max', 'Fire_Danger',
    #                            'Color']
    # else:
    #     result_ffwi.columns = ['Name', 'Lat', 'Long', 'Date', 'FWI_lin', 'FWI_near', 'FWI_max', 'Fire_Danger', 'Color']

    return (result_ffwi)


#--------------------------------------------------------------------------------------------------
# Calculates the generalized (power) mean
#
def generalized_mean(freqs, scale, p):

    from math import pow

    len_sc = len(scale)
    len_freqs = len(freqs)

    if len_freqs == len_sc and p != 0:
        sum_freq_sc = 0
        for i in range(0, len_freqs):
            sum_freq_sc = sum_freq_sc + freqs[i]*pow(scale[i], p)

        sum_freq = sum(freqs)

        gen_mean = pow( sum_freq_sc/sum_freq, 1.0/p)
    else:
        print("\n The generalized mean could not be calculated. Either p is non zero "
              "either the length of scale is different from freqs!!!")
        gen_mean = None

    return(gen_mean)

#--------------------------------------------------------------------------
# Obsolete version for Simple Fire Danger Index
#
# def Fire_Overall_Crisis_Level(ds):
#
#     from math import ceil
#
#     # Low -> 1, Moderate -> 2, ..., Extreme -> 5
#     scale = [1,2,3,4,5]
#     categories = ['Low', 'Moderate', 'High', 'Very High', 'Extreme' ]
#     p = len(scale)
#
#     # Calculate the frequency (count) of each category
#     counts = [0]*len(scale)
#
#     for it in range(len(categories)):
#         temp_ds = DataFrame( ds.loc[ ds['Fire_Risk'] == categories[it] ] )
#         counts[it] = temp_ds.shape[0]
#     #    print("Category=", categories[it], " + ", counts[it]  )
#     #print("-----------------------------\n")
#
#     focl_indx = ceil( generalized_mean(counts, scale, p) )
#
#     if focl_indx == 1:
#         col = '#00FF00'  # green
#         note = 'Low'
#     elif focl_indx == 2:
#         col = '#4682B4'  # steelblue'
#         note = "Moderate"
#     elif focl_indx == 3:
#         col = '#FF8000'  # orange
#         note = "High"
#     elif focl_indx == 4:
#         col = '#FF0000'  # red
#         note = "Very High"
#     else:
#         col = '#8B0000'  # red darked
#         note = "Extreme"
#
#     focl_dict = {'val' : focl_indx, 'color': col, 'note': note}
#
#     return( focl_dict )

# New Version:
#
def Fire_Overall_Crisis_Level(ds):

    from math import ceil

    # Very Low -> 1, Low -> 2, Moderate -> 3, ..., Extreme -> 6
    scale = [1,2,3,4,5,6]
    categories = ['Very Low', 'Low', 'Moderate', 'High', 'Very High', 'Extreme' ]
    p = len(scale)

    # Calculate the frequency (count) of each category
    counts = [0]*len(scale)

    for it in range(len(ds)):
         counts[it] = ds[it]['Count']
         print("Category=", categories[it], " + ", counts[it]  )
    print("-----------------------------\n")

    focl_indx = ceil( generalized_mean(counts, scale, p) )

    if focl_indx == 1:
        col = '#00FF00'  # green
        note = 'Very Low'
    elif focl_indx == 2:
        col = '#FFFF00' # yellow
        note = 'Low'
    elif focl_indx == 3:
        col = '#FFC125'  # goldenrod 1
        note = "Moderate"
    elif focl_indx == 4:
        col = '#FF8000'  # orange
        note = "High"
    elif focl_indx == 5:
        col = '#FF0000'  # red
        note = "Very High"
    else:
        col = '#8B0000'  # red darked
        note = "Extreme"

    focl_dict = {'val': focl_indx, 'color': col, 'note': note, 'val_rescale': focl_indx/6*100}

    return( focl_dict )


#---------------------------------------------------------------------
def open_netcdf(fname):
    if fname.endswith(".gz"):
        infile = gzip.open(fname, 'rb')
        tmp = tempfile.NamedTemporaryFile(delete=False)
        shutil.copyfileobj(infile, tmp)
        infile.close()
        tmp.close()
        data = Dataset(tmp.name)
        os.unlink(tmp.name)
    else:
        data = netCDF4.Dataset(fname)
    return data


def get_nc_file_contents(data, local_field_name):
    '''Read and return content from a single .nc file
       for local_field_name == 'pres' the units are Pascals
       For local_field_name == 'pr_wtr' the units are ???
    '''
    nc_fid = data
    lats = nc_fid.variables['lat'][:]
    lons = nc_fid.variables['lon'][:]
    time = nc_fid.variables['time']
    time = num2date(time[:], time.units)
    field_value = nc_fid.variables[local_field_name][:,:,:]
    return lons, lats, time, field_value


def geo_idx(dd, dd_array):
   """
     search for nearest decimal degree in an array of decimal degrees and return the index.
     np.argmin returns the indices of minium value along an axis.
     so subtract dd from all values in dd_array, take absolute value and find index of minium.
    """
   geo_idx_num = (np.abs(dd_array - dd)).argmin()
   return geo_idx_num

# OBSOLETE CODE UP TO VERSION 8
# def calc_Index(lons, lats, fieldvalues, x0, y0):
#     y0 = 360 + y0
#     lat_idx = geo_idx(x0, lats)
#     lon_idx = geo_idx(y0, lons)
#     xx, yy = [lats[lat_idx -1 :lat_idx + 2], lons[lon_idx -1:lon_idx + 2]]
#     xx, yy = np.meshgrid(xx, yy)
#     points = np.vstack([xx.ravel(), yy.ravel()]).T
#     values = fieldvalues[0, lat_idx -1 :lat_idx + 2, lon_idx -1:lon_idx + 2].flatten('F')
#
#     z_int_lin = griddata(points, values, (x0, y0), method = 'linear')
#     z_int_near = griddata(points, values, (x0, y0), method = 'nearest')
# #    z_int_cubic = griddata(points, values, (x0, y0), method = 'cubic')
#
#     z_int_max = np.amax(values)
#
# #    z_int = np.array([z_int_lin, z_int_near, z_int_cubic, z_int_max])
#     z_int = np.array([z_int_lin, z_int_near, z_int_max])
#
#     return z_int

# NEW VERSION 9
def calc_Index(lons, lats, fieldvalues, x0, y0, no_pnt):
    y0 = 360 + y0
    lat_idx = geo_idx(x0, lats)
    lon_idx = geo_idx(y0, lons)
    xx, yy = [lats[lat_idx - no_pnt:lat_idx + no_pnt], lons[lon_idx - no_pnt:lon_idx + no_pnt]]
    xx, yy = np.meshgrid(xx, yy)
    points = list(np.vstack([xx.ravel(), yy.ravel()]).T)
    values = list(fieldvalues[0, lat_idx - no_pnt:lat_idx + no_pnt, lon_idx - no_pnt:lon_idx + no_pnt].flatten('F'))

    i = 0
    while i < len(values):
        if np.ma.is_masked(values[i]):
            values.pop(i)
            points.pop(i)
            i -= 1
        i += 1

    z_int_cubic = griddata(points, values, (x0, y0), method='cubic')
    z_int_lin = griddata(points, values, (x0, y0), method='linear')
    z_int_near = griddata(points, values, (x0, y0), method='nearest')
    z_int_max = np.amax(values)
    z_int_min = np.amin(values)
    z_int_std = np.std(values)
    z_int_mean = np.mean(values)

    z_int = np.array([z_int_lin, z_int_near, z_int_cubic, z_int_max, z_int_min, z_int_std, z_int_mean])

    return z_int

#------SWSTH------------------------------------------
# def get_ftp(url, Username, Password):
#     # path to store the fetched files from ftp
#     PATH = os.getcwd()
#
#     # connect with ftp
#     ftp = FTP(url)
#     ftp.login(Username, Password)
#     print("Logged in: ", url)
#
#     # find the file which
#     ls = []
#     ftp.retrlines('LIST', ls.append)
#
#     fwi_filesdates = []
#     flag_find = False
#
#     if len(ls) > 0:
#        for it in range(len(ls)):
#             if ls[it].endswith("_fwishort.tar"):
#                 fwi_filesdates.append(int(ls[it].split()[-1].split("_")[0]))
#                 maxdate = np.amax(fwi_filesdates)
#                 flag_find = True
#
#     # If no fwishort files have been found to the ftp list check local list
#     if flag_find == False:
#         local_list = os.listdir(PATH)
#         for it in range(len(local_list)):
#             if local_list[it].endswith("_fwishort.tar"):
#                 fwi_filesdates.append(int(local_list[it].split()[-1].split("_")[0]))
#                 maxdate = np.amax(fwi_filesdates)
#
#     flname = str(maxdate) + '_fwishort.tar'
#     PATH_flname = PATH + "/" + flname
#
#     print("flname=", flname )
#     print("PATH_flname = ", PATH_flname)
#
#     # Start Download process if it is needed.
#     if flag_find == True:
#         # Download the current file, if it already exist=true or the size is same as FTP link file size, dont download
#
#         # check the size, if same= True
#         flag = False
#         if os.path.isfile(PATH_flname):
#             b = os.path.getsize(PATH_flname)
#             for i in range(len(ls)):
#                 if str(b) in ls[i].split()[4]:
#                     flag = True  # exist file with same name and size
#
#         if flag == True:
#             print("File already exists and has same size as ftp files")
#         else:
#             start_time = time.time()
#
#             print("Downloading...")
#             # Download
#             retr_file = 'RETR' + " " + flname
#             ftp.retrbinary(retr_file, open(flname, 'wb').write)
#
#             # unrar
#             tar = tarfile.open(PATH_flname)
#             tar.extractall()
#             tar.close()
#
#             # how much time used for download
#             elapsed_time = time.time() - start_time
#             ftp_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
#             print('Time needed for downloading the file: {}'.format(ftp_time))
#
#
#     res = {'PATH': PATH, 'Filename': flname, 'Date': maxdate}
#
#     print(res)
#
#     ftp.quit()
#     print("File downloaded at: ", PATH)
#     return res

#------REVIEW
def get_ftp(url, Username, Password):

    # path to store the fetched files from ftp
    # PATH = os.getcwd()
    current_dirs_parent = os.getcwd()
    print(current_dirs_parent)
    PATH = current_dirs_parent + "/" + "CRCL/FireCRisisCLassification" + "/"
    print(PATH)
    # connect with ftp
    ftp = FTP(url)
    ftp.login(Username, Password)
    print("Logged in: ", url)

    # find the file which
    # ls =['-rw-r--r--    1 nen      ec        197457920 Jun 15 12:51 20180611_fwishort.tar']
    # ftp.retrlines('LIST', ls.append)

    fwi_filesdates = []
    flag_find = False

    #     if len(ls) > 0:
    #        for it in range(len(ls)):
    #             if ls[it].endswith("_fwishort.tar"):
    #                 fwi_filesdates.append(int(ls[it].split()[-1].split("_")[0]))
    #                 maxdate = np.amax(fwi_filesdates)
    #                 flag_find = True

    # If no fwishort files have been found to the ftp list check local list
    # if flag_find == False:
    flag = False
    local_list = os.listdir(PATH)
    for it in range(len(local_list)):
        if local_list[it].endswith("_fwishort.tar"):
            fwi_filesdates.append(int(local_list[it].split()[-1].split("_")[0]))
            maxdate = np.amax(fwi_filesdates)
            flag = True

    flname = str(maxdate) + '_fwishort.tar'
    PATH_flname = PATH + flname

    print("flname=", flname)
    print("PATH_flname = ", PATH_flname)

    # Start Download process if it is needed.
    # if flag_find == True:
    # Download the current file, if it already exist=true or the size is same as FTP link file size, dont download

    # check the size, if same= True
    #     flag = False
    #     if os.path.isfile(PATH_flname):
    #         b = os.path.getsize(PATH_flname)
    #         for i in range(len(ls)):
    #             if str(b) in ls[i].split()[4]:
    #                 flag = True  # exist file with same name and size

    if flag == True:
        print("File already exists and has same size as ftp files")
        tar = tarfile.open(PATH_flname)
        for member in tar.getmembers():
            if not os.path.exists(PATH+member.name):
                tar.extractall(PATH)
        tar.close()
    else:
        start_time = time.time()

        print("Downloading...")
        # Download
        retr_file = 'RETR' + " " + flname
        ftp.retrbinary(retr_file, open(flname, 'wb').write)

        # unrar
        tar = tarfile.open(PATH_flname)
        tar.extractall(PATH)
        tar.close()

        # how much time used for download
        elapsed_time = time.time() - start_time
        ftp_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        print('Time needed for downloading the file: {}'.format(ftp_time))

    res = {'PATH': PATH, 'Filename': flname, 'Date': maxdate}

    ftp.quit()
    print("File downloaded at: ", PATH)
    return res

#------------------------

#---------------------------------------------------------------------
# Function to get big files ###_hr.tar
#
def get_ftp_v2(url, Username, Password):
    PATH = os.getcwd()

    url='dissemination.ecmwf.int'
    Username='fire'
    Password='fire2017'

    ftp = FTP(url)
    ftp.login(Username, Password)
    print("Logged in: ", url)


    # find the file which
    ls = []
    #ls = ['qwe','daw']
    ftp.retrlines('LIST', ls.append)

    fwi_filesdates = []
    for it in range(len(ls)):
        if ls[it].endswith('1200_hr.tar'):
            fwi_filesdates.append(int(ls[it].split("_")[2]))
            maxdate = np.amax(fwi_filesdates)

    flname = 'ECMWF_EFFIS_'+str(maxdate) + '_1200_hr.tar'
    PATH_flname = PATH + "/" + flname

    # Download the current file, if it allready exist=true or the size is same as FTP link file size, dont download

    # check the size, if same= True
    flag = False
    if os.path.isfile(PATH_flname):
        b = os.path.getsize(PATH_flname)
        for i in range(len(ls)):
            if str(b) in ls[i].split()[4]:
                flag = True  # exist file with same name and size

    if flag == True:
        print("File already exists and has same size as ftp files")
    else:
        start_time = time.time()

        print("Downloading...")
        # Download
        retr_file = 'RETR' + " " + flname
        ftp.retrbinary(retr_file, open(flname, 'wb').write)

        # unrar
        tar = tarfile.open(PATH_flname)
        tar.extractall()
        tar.close()

        # how much time used for download
        elapsed_time = time.time() - start_time
        ftp_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        print('Time needed for downloading the file: {}'.format(ftp_time))

    res = {'PATH': PATH, 'Filename': flname, 'Date': maxdate}

    ftp.quit()
    print("File downloaded at: ", PATH)
    return res




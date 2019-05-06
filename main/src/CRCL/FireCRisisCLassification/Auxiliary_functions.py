import json, time, re
import os, errno, sys
import glob, gzip, pickle, shutil, tempfile, re, tarfile
import numpy as np
import pandas as pd

from scipy.interpolate import griddata
from scipy.interpolate import Rbf
from scipy import interpolate
from netCDF4 import Dataset, num2date
import netCDF4
#from bus.bus_producer import BusProducer
#from bus.CRCL_service import CRCLService
from pathlib import Path
from datetime import datetime, timedelta
from ftplib import FTP

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

    print(result_ffwi.shape)
    if result_ffwi.shape[1] == 14:
        result_ffwi.columns = ['index', 'Name', 'Lat', 'Long', 'Date', 'FWI_lin', 'FWI_near', 'FWI_cubic', 'FWI_max',
                               'FWI_min',
                               'FWI_std', 'FWI_mean', 'Fire_Danger', 'Color']
    else:
        result_ffwi.columns = ['Name', 'Lat', 'Long', 'Date', 'FWI_lin', 'FWI_near', 'FWI_cubic', 'FWI_max', 'FWI_min',
                               'FWI_std', 'FWI_mean', 'Fire_Danger', 'Color']

    return (result_ffwi)


# --------------------------------------------------------------------------------------------------
# Calculates the generalized (power) mean
#
def generalized_mean(freqs, scale, p):
    from math import pow

    len_sc = len(scale)
    len_freqs = len(freqs)

    if len_freqs == len_sc and p != 0:
        sum_freq_sc = 0
        for i in range(0, len_freqs):
            sum_freq_sc = sum_freq_sc + freqs[i] * pow(scale[i], p)

        sum_freq = sum(freqs)

        gen_mean = pow(sum_freq_sc / sum_freq, 1.0 / p)
    else:
        print("\n The generalized mean could not be calculated. Either p is non zero "
              "either the length of scale is different from freqs!!!")
        gen_mean = None

    return (gen_mean)


# New Version:
#
def Fire_Overall_Crisis_Level(ds):
    from math import ceil

    # Very Low -> 1, Low -> 2, Moderate -> 3, ..., Extreme -> 6
    scale = [1, 2, 3, 4, 5, 6]
    categories = ['Very Low', 'Low', 'Moderate', 'High', 'Very High', 'Extreme']
    p = len(scale)

    # Calculate the frequency (count) of each category
    counts = [0] * len(scale)

    for it in range(len(ds)):
        counts[it] = ds[it]['Count']
        print("Category=", categories[it], " + ", counts[it])
    print("-----------------------------\n")

    focl_indx = ceil(generalized_mean(counts, scale, p))

    if focl_indx == 1:
        col = '#00FF00'  # green
        note = 'Very Low'
    elif focl_indx == 2:
        col = '#FFFF00'  # yellow
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

    focl_dict = {'val': focl_indx, 'color': col, 'note': note}

    return (focl_dict)


# ---------------------------------------------------------------------
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
    field_value = nc_fid.variables[local_field_name][:, :, :]
    # time=time[:]
    #     time=time[0].isoformat()

    return lons, lats, time, field_value


def geo_idx(dd, dd_array):
    """
      search for nearest decimal degree in an array of decimal degrees and return the index.
      np.argmin returns the indices of minium value along an axis.
      so subtract dd from all values in dd_array, take absolute value and find index of minium.
     """
    geo_idx_num = (np.abs(dd_array - dd)).argmin()
    return geo_idx_num


##combine the methods
def calc_Index(dts, lons, lats, fieldvalues, x0, y0, no_pnt):
    y0 = 360 + y0
    lat_idx = geo_idx(x0, lats)
    lon_idx = geo_idx(y0, lons)
    xx, yy = [lats[lat_idx - no_pnt:lat_idx + no_pnt], lons[lon_idx - no_pnt:lon_idx + no_pnt]]
    xx, yy = np.meshgrid(xx, yy)
    points = list(np.vstack([xx.ravel(), yy.ravel()]).T)
    values = list(fieldvalues[dts, lat_idx - no_pnt:lat_idx + no_pnt, lon_idx - no_pnt:lon_idx + no_pnt].flatten('F'))

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


#====================================================================
def get_ftp(url, Username, Password, directory_forNC):

    PATH = os.getcwd()

    #keep the normal os
    currdir = os.getcwd()

    # connect with ftp
    ftp = FTP(url)
    ftp.login(Username, Password)
    print("Logged in: ", url)

    # find the file which
    ls = []
    ftp.retrlines('LIST', ls.append)

    fwi_filesdates = []
    flag_find = False

    if len(ls) > 0:
        for it in range(len(ls)):
            if ls[it].endswith("_1200_hr.nc"):
                fwi_filesdates.append(int(ls[it].split()[-1].split("_")[3]))
                maxdate = np.amax(fwi_filesdates)
                flag_find = True

    # If no 1200.nc files have been found to the ftp list check local list
    if flag_find == False:
        local_list = os.listdir(PATH)
        flag = False
        for it in range(len(local_list)):
            if local_list[it].endswith("_1200_hr.nc"):
                fwi_filesdates.append(int(local_list[it].split()[-1].split("_")[3]))
                maxdate = np.amax(fwi_filesdates)
                flag = True

    flname = "ECMWF_EFFIS_FWI_" + str(maxdate) + '_1200_hr.nc'  # "ECMWF_EFFIS_FWI_20180712_1200_hr.nc"
    PATH_flname = directory_forNC + flname

    print("flname=", flname)
    print("PATH_flname = ", PATH_flname)

    # change the os to the directory we want to dll
    os.chdir(directory_forNC)

    # Start Download process if it is needed.
    if flag_find == True:
        # Download the current file, if it already exist=true or the size is same as FTP link file size, dont download

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

        #      #unrar
        #     tar = tarfile.open(PATH_flname)
        #     tar.extractall()
        #     tar.close()

        # how much time used for download
        elapsed_time = time.time() - start_time
        ftp_time = time.strftime("%H:%M:%S", time.gmtime(elapsed_time))
        print('Time needed for downloading the file: {}'.format(ftp_time))

    res = {'PATH_flname': PATH_flname, 'Filename': flname, 'Date': maxdate}

    #Change os to normal
    os.chdir(currdir)
    ftp.quit()
    print("File downloaded at: ", PATH_flname)
    return res

from pandas import DataFrame, concat
import numpy as np
from datetime import datetime, timedelta, timezone

#-------------------------------------------------------------------------------
#   OBSOLETE VERSION
#
# def Heatwave_Disconfort_Index(df, T0, a0):
#
#     DI = []
#     size_df = df.shape
#     for i in range(size_df[0]):
#         di_val = df['Temp_C'].iloc[i] - a0 * (1 - 0.01 * df['Rel_Humidity'].iloc[i]) * (df['Temp_C'].iloc[i] - T0)
#         DI.append( round(di_val, 3) )
#
#     HeatDS = DataFrame(df, columns=['Measurement_Number', 'DateTime', 'Lat', 'Long', 'Temp_C', 'Rel_Humidity'])
#     HeatDS['DI'] = DI
#
#     # Calculate the Heatwave Risk based on Discomfort Index (DI)
#
#     Heat_Risk = []
#     Color = []
#
#     for i in range(HeatDS.shape[0]):
#         if HeatDS['DI'].iloc[i] >= 32.0:
#             Heat_Risk.append('Very Strong Discomfort')
#             Color.append('#FF0000')  # red
#         elif HeatDS['DI'].iloc[i] >= 30.0 and HeatDS['DI'].iloc[i] < 32.0:
#             Heat_Risk.append('Heavy Discomfort')
#             Color.append('#FF8000')  # orange
#         elif HeatDS['DI'].iloc[i] >= 28.0 and HeatDS['DI'].iloc[i] < 30.0:
#             Heat_Risk.append('Most population feels discomfort')
#             Color.append('#FFD700')  # gold
#         elif HeatDS['DI'].iloc[i] >= 25.0 and HeatDS['DI'].iloc[i] < 28.0:
#             Heat_Risk.append('More than half population feels discomfort')
#             Color.append('#FFFF00')  # yellow
#         elif HeatDS['DI'].iloc[i] >= 21.0 and HeatDS['DI'].iloc[i] < 25.0:
#             Heat_Risk.append('Less than half population feels discomfort')
#             Color.append('#4682B4')  # steelblue
#         else:                           # HeatDS['DI'].iloc[i] < 21.0
#             Heat_Risk.append('No discomfort')
#             Color.append('#0000FF')  # blue
#
#     HeatDS['DI_Category'] = Heat_Risk
#     HeatDS['Color'] = Color
#
#     return(HeatDS)

# New Version of function after Ver 3

def Heatwave_Disconfort_Index(df, T0, a0):

    DI = []
    size_df = df.shape
    for i in range(size_df[0]):
        di_val = df['Temperature'].iloc[i] - a0 * (1 - 0.01 * df['Humidity'].iloc[i]) * (df['Temperature'].iloc[i] - T0)
        DI.append( round(di_val, 3) )

    HeatDS = DataFrame(df, columns= list(df.columns.values))
    HeatDS['DI'] = DI

    # Calculate the Heatwave Risk based on Discomfort Index (DI)

    Heat_Risk = []
    Color = []

    for i in range(HeatDS.shape[0]):
        if HeatDS['DI'].iloc[i] >= 32.0:
            Heat_Risk.append('Very Strong Discomfort')
            Color.append('#FF0000')  # red
        elif HeatDS['DI'].iloc[i] >= 30.0 and HeatDS['DI'].iloc[i] < 32.0:
            Heat_Risk.append('Heavy Discomfort')
            Color.append('#FF8000')  # orange
        elif HeatDS['DI'].iloc[i] >= 28.0 and HeatDS['DI'].iloc[i] < 30.0:
            Heat_Risk.append('Most population feels discomfort')
            Color.append('#FFD700')  # gold
        elif HeatDS['DI'].iloc[i] >= 25.0 and HeatDS['DI'].iloc[i] < 28.0:
            Heat_Risk.append('More than half population feels discomfort')
            Color.append('#FFFF00')  # yellow
        elif HeatDS['DI'].iloc[i] >= 21.0 and HeatDS['DI'].iloc[i] < 25.0:
            Heat_Risk.append('Less than half population feels discomfort')
            Color.append('#4682B4')  # steelblue
        else:                           # HeatDS['DI'].iloc[i] < 21.0
            Heat_Risk.append('No discomfort')
            Color.append('#0000FF')  # blue

    HeatDS['DI_Category'] = Heat_Risk
    HeatDS['Color'] = Color

    return(HeatDS)



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
def Heatwave_Overall_Crisis_Level(ds):

    from math import ceil

    scale = [0,1,1,2,3,4]
    categories = [ 'No discomfort',
                   'Less than half population feels discomfort',
                   'More than half population feels discomfort',
                   'Most population feels discomfort',
                   'Heavy Discomfort',
                   'Very Strong Discomfort' ]
    p = len( list(set(scale)) )

    # Calculate the frequency (count) of each category
    counts = [0]*len(scale)

    for it in range(len(categories)):
        temp_ds = DataFrame( ds.loc[ ds['DI_Category'] == categories[it] ] )
        counts[it] = temp_ds.shape[0]
        # print("\n Category=", categories[it], " + ", counts[it]  )

    hocl_indx = ceil( generalized_mean(counts, scale, p) )

    # print("hocl_indx =", hocl_indx )

    if hocl_indx == 0:
        col = '#0000FF'#
        note = 'Normal'
        val_hocl_indx = 0
    elif hocl_indx == 1:
        col = '#FFFF00'  # yellow
        note = 'Warm'
        val_hocl_indx = hocl_indx/4*100
    elif hocl_indx == 2:
        col = '#FFD700' # gold
        note = "Hot"
        val_hocl_indx = hocl_indx / 4 * 100
    elif hocl_indx == 3:
        col = 'FF8000'   # orange
        note = 'Very Hot'
        val_hocl_indx = hocl_indx / 4 * 100
    else:
        col = '#FF0000'   # red
        note = "Extreme"
        val_hocl_indx = hocl_indx / 4 * 100


    hocl_dict = {'val' : val_hocl_indx, 'color': col, 'note': note}

    return( hocl_dict )

#-----------------------------------------------------------------------
# Function to convert Date/Time to current Date/Time of a dataframe
#   ONLY FOR THE EMERGENCY PHASE
#
# POSSIBLE WRONG
#
# def convert_datetime_to_current(dset):
#
#     dt = dset['DateTime'].unique()
#
#     cur_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
#
#     diff = cur_date - datetime.strptime(dt[0], "%Y-%m-%dT%H:%M:%S")
#
#     newdt = []
#     newep = []
#     for it in range(len(dt)):
#         cur_date = (datetime.strptime(dt[it], "%Y-%m-%dT%H:%M:%S") + diff).isoformat()
#
#         td = datetime.strptime(cur_date, '%Y-%m-%dT%H:%M:%S')
#
#         epoch_date = int(datetime(td.year, td.month, td.day, td.hour, td.minute, td.second).timestamp())
#
#         nrows = len(dset[dset['DateTime'] == dt[it]])
#
#         for r in range(nrows):
#             newdt.append(cur_date)
#             newep.append(epoch_date)
#
#     dset = dset.drop('DateTime', 1)
#     dset['DateTime'] = newdt
#
#     return(dset)

def convert_datetime_to_current(dset):
    dt = dset['DateTime'].unique()
    cur_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    diff = cur_date - datetime.strptime(dt[0], "%Y-%m-%dT%H:%M:%S")

    newdt = []
    newep = []
    pr = dset['Name'].unique()
    for i in range(len(pr)):

        for it in range(len(dt)):
            cur_date = (datetime.strptime(dt[it], "%Y-%m-%dT%H:%M:%S") + diff).isoformat()

            td = datetime.strptime(cur_date, '%Y-%m-%dT%H:%M:%S')

            epoch_date = int(datetime(td.year, td.month, td.day, td.hour, td.minute,td.second).timestamp())


            newdt.append(cur_date)
            newep.append(epoch_date)


    dset = dset.drop('DateTime', 1)
    dset['DateTime'] = newdt

    return(dset)

#-------------------------------------------------------------------------
# Find the set of records that are nearest to the date/time
#
def find_df_of_nearest_time(cur_simdf):

    current_date = datetime.utcnow().replace(microsecond=0)
    epoch_current_date = int(current_date.timestamp())

    dt = cur_simdf['DateTime'].unique()

    diff_epochs = np.array([])
    for i in range(len(dt)):
        temp_dt = datetime.strptime(dt[i], '%Y-%m-%dT%H:%M:%S')
        ep_temp_dt = int(temp_dt.timestamp())

        diff_temp = ep_temp_dt - epoch_current_date

        diff_epochs = np.append(diff_epochs, [abs(diff_temp)])

    pos_min = np.where(diff_epochs == diff_epochs.min())[0]

    # Measurement_Number starts counting from 1
    pos_df = pos_min[0] + 1

    dset = cur_simdf.loc[cur_simdf['Measurement_Number'] == pos_df]

    return(dset)

#==============================================================================
# Pre-Emergency HW phase:
#
def convert_to_current_date_time(dset, cur_date):

    # Convert date/time and forecast interval to current ones
    dt = dset['DateTime'].unique()

    diff = cur_date - datetime.strptime(dt[0], "%Y-%m-%dT%H:%M:%S")

    newdt = []
    newep = []
    pr = dset['Name'].unique()
    for i in range(len(pr)):

        for it in range(len(dt)):
            dtit = (datetime.strptime(dt[it], "%Y-%m-%dT%H:%M:%S") + diff).isoformat()
            td = datetime.strptime(dtit, '%Y-%m-%dT%H:%M:%S')
            epoch_date = int(datetime(td.year, td.month, td.day, td.hour, td.minute, td.second).timestamp())

            newdt.append(dtit)
            newep.append(epoch_date)

    dset = dset.drop('DateTime', 1)
    dset['DateTime'] = newdt
    dset = dset.drop('Epoch', 1)
    dset['Epoch'] = newep

    return(dset)

#-------------------------------------------------------------------------------
def find_nearest_df_to_current_hour(dset, current_date):

    current_hour = current_date.hour

    dt = dset['DateTime']

    diff_hours = list()
    for i in range(len(dt)):
        temp_dt = datetime.strptime(dt[i], '%Y-%m-%dT%H:%M:%S')
        temp_hour = temp_dt.hour

        difh = temp_hour - current_hour

        diff_hours.append(abs(difh))

        if difh == 0:
            pos_min = i

            temp = dset.iloc[0:pos_min]
            temp.reset_index(drop=True, inplace=True)

            newdata = dset.iloc[(pos_min):dset.shape[0]]
            newdata.reset_index(drop=True, inplace=True)

            break

    # outside for - Code for recycle temp records by changing DateTime and append to the newdata
    # nr = newdata.shape[0]
    # last_newdata_dt = newdata['DateTime'].iloc[nr - 1]
    #
    # new_temp_dt0 = datetime.strptime(last_newdata_dt, '%Y-%m-%dT%H:%M:%S')
    #
    # new_dts = list()
    # new_epochs = list()
    #
    # unq_temp_dt = temp['DateTime'].unique()
    #
    # for ith in range(len(unq_temp_dt)):
    #     new_temp_dt = new_temp_dt0 + timedelta(hours=ith + 1)
    #     new_temp_ep = int(datetime(new_temp_dt.year, new_temp_dt.month, new_temp_dt.day,
    #                                new_temp_dt.hour, new_temp_dt.minute, new_temp_dt.second).timestamp())
    #     for j in range(0, 6):
    #         new_epochs.append(new_temp_ep)
    #         new_dts.append(new_temp_dt.isoformat())
    #
    # temp = temp.drop('DateTime', 1)
    # temp['DateTime'] = new_dts
    # temp = temp.drop('Epoch', 1)
    # temp['Epoch'] = new_epochs

    # newdata = concat([newdata, temp], axis=0, ignore_index=True)

    return (newdata)
from pandas import DataFrame

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

    scale = [1,1,1,2,3,4]
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
    #    print("Category=", categories[it], " + ", counts[it]  )

    hocl_indx = ceil( generalized_mean(counts, scale, p) )

    if hocl_indx == 1:
        col = '#FFFF00'  # yellow
        note = 'Warm'
    elif hocl_indx == 2:
        col = '#FFD700' # gold
        note = "Hot"
    elif hocl_indx == 3:
        col = 'FF8000'   # orange
        note = 'Very Hot'
    else:
        col = '#FF0000'   # red
        note = "Extreme"

    #hocl_dict = {'val' : hocl_indx, 'color': col, 'note': note}
    hocl_dict = {'val' : hocl_indx/4*100, 'color': col, 'note': note}

    return( hocl_dict )

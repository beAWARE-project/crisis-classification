import pandas as pd
from math import pow, ceil


def compare_value_thresholds(value, StID, dstr_ind, Thresholds):

    StationID = StID[0]
    dstr_indicator = dstr_ind[0]

    if dstr_indicator == 'Water':
        # find the element of the Thresholds (_WL dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):

            if StationID == str(item['ID']):
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     MCol_WL = ['#31A34F']  # green
                     MNote_WL = ['Water Level OK - Moderate Crisis Level']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_WL = ['#F8E71C']  # yellow
                     MNote_WL = ['Water Level exceeds 1st alarm threshold - Medium Crisis Level']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_WL = ['#E68431']  # orange
                     MNote_WL = ['Water Level exceeds 2nd alarm threshold - High Crisis Level']
                 else:  # value >= item['Alarm3']:
                     MCol_WL = ['#AA2050'] # red
                     MNote_WL = ['Water Level exceeds 3rd alarm threshold - Very High Crisis Level']


        MColNote_WL = [MCol_WL, MNote_WL]

        return MColNote_WL

    if dstr_indicator == 'Precipitation':
        # find the element of the Thresholds (_PR dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):
            if StationID == str(item['ID']):
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     MCol_PR = ['#31A34F']  # green
                     MNote_PR = ['Precipitation Level OK - Moderate Crisis Level']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_PR = ['#F8E71C']  # yellow
                     MNote_PR = ['Precipitation Level exceeds 1st alarm threshold - Medium Crisis Level']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_PR = ['#E68431']  # orange
                     MNote_PR = ['Precipitation Level exceeds 2nd alarm threshold - High Crisis Level']
                 else:  # value >= item['Alarm3']:
                     MCol_PR = ['#AA2050'] # red
                     MNote_PR = ['Precipitation Level exceeds 3rd alarm threshold - Very High Crisis Level']

        MColNote_PR = [MCol_PR, MNote_PR]

        return MColNote_PR

#---------------------------------------------------------------------------------------
# Compare value with alarm thresholds and return a color, note, scale and scale_note
#
def compare_value_scale_thresholds(value, StID, dstr_ind, Thresholds):

    StationID = StID[0]
    dstr_indicator = dstr_ind[0]

    if dstr_indicator == 'Water':
        # find the element of the Thresholds (_WL dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):

            if StationID == str(item['ID']):
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     MCol_WL = ['#31A34F']  # green
                     MNote_WL = ['Water Level OK - Moderate Crisis Level']
                     #Scale_WL = 0 #['0']
                     Scale_WL = 1 #['1']
                     Note_Scale_WL = ['Low']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_WL = ['#F8E71C']  # yellow
                     MNote_WL = ['Water Level exceeds 1st alarm threshold - Medium Crisis Level']
                     #Scale_WL = 1 #['1']
                     Scale_WL = 2 #['2']
                     Note_Scale_WL = ['Medium']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_WL = ['#E68431']  # orange
                     MNote_WL = ['Water Level exceeds 2nd alarm threshold - High Crisis Level']
                     #Scale_WL = 2 #['2']
                     Scale_WL = 3 #['3']
                     Note_Scale_WL = ['High']
                 else:  # value >= item['Alarm3']:
                     MCol_WL = ['#AA2050'] # red
                     MNote_WL = ['Water Level exceeds 3rd alarm threshold - Very High Crisis Level']
                     #Scale_WL = 3 #['3']
                     Scale_WL = 4 #['4']
                     Note_Scale_WL = ['Very High']

        MColNote_WL = [MCol_WL, MNote_WL, Scale_WL, Note_Scale_WL]

        return MColNote_WL

    if dstr_indicator == 'Precipitation':
        # find the element of the Thresholds (_PR dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):
            if StationID == str(item['ID']):
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     MCol_PR = ['#31A34F']  # green
                     MNote_PR = ['Precipitation Level OK - Moderate Crisis Level']
                     #Scale_PR = 0 #['0']
                     Scale_PR = 1 #['1']
                     Note_Scale_PR = ['Low']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_PR = ['#F8E71C']  # yellow
                     MNote_PR = ['Precipitation Level exceeds 1st alarm threshold - Medium Crisis Level']
                     #Scale_PR = 1 #['1']
                     Scale_PR = 2 #['2']
                     Note_Scale_PR = ['Medium']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_PR = ['#E68431']  # orange
                     MNote_PR = ['Precipitation Level exceeds 2nd alarm threshold - High Crisis Level']
                     #Scale_PR = 2 #['2']
                     Scale_PR = 3 #['3']
                     Note_Scale_PR = ['High']
                 else:  # value >= item['Alarm3']:
                     MCol_PR = ['#AA2050'] # red
                     MNote_PR = ['Precipitation Level exceeds 3rd alarm threshold - Very High Crisis Level']
                     #Scale_PR = 3 #['3']
                     Scale_PR = 4 #['4']
                     Note_Scale_PR = ['Very High']

        MColNote_PR = [MCol_PR, MNote_PR, Scale_PR, Note_Scale_PR]

        return MColNote_PR


#---------------------------------------------------------------------------------------
# Compare value with alarm thresholds and return a color, note, scale and scale_note
#
def compare_value_scale_thresholds_new(value, StationID, dstr_ind, Thresholds):

    dstr_indicator = dstr_ind[0]

    if dstr_indicator == 'Water':
        # find the element of the Thresholds (_WL dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):

            if StationID == item['ID']:
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     MCol_WL = '#31A34F'  # green
                     MNote_WL = 'Water Level OK '
                     #Scale_WL = 0 #['0']
                     Scale_WL = 1 #['1']
                     Note_Scale_WL = 'Low'
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_WL = '#F8E71C'  # yellow
                     MNote_WL = 'Water Level exceeds 1st alarm threshold '
                     #Scale_WL = 1 #['1']
                     Scale_WL = 2 #['2']
                     Note_Scale_WL = 'Medium'
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_WL = '#E68431'  # orange
                     MNote_WL = 'Water Level exceeds 2nd alarm threshold '
                     #Scale_WL = 2 #['2']
                     Scale_WL = 3 #['3']
                     Note_Scale_WL = 'High'
                 else:  # value >= item['Alarm3']:
                     MCol_WL = '#AA2050' # red
                     MNote_WL = 'Water Level exceeds 3rd alarm threshold '
                     #Scale_WL = 3 #['3']
                     Scale_WL = 4 #['4']
                     Note_Scale_WL = 'Very High'


        MColNote_WL = [MCol_WL, MNote_WL, Scale_WL, Note_Scale_WL]

        return MColNote_WL

    if dstr_indicator == 'Precipitation':
        # find the element of the Thresholds (_PR dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):
            if StationID == item['ID']:
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     Col_PR = '#31A34F'  # green
                     Note_PR = 'Precipitation Level OK '
                     #Scale_PR = 0 #['0']
                     Scale_PR = 1 #['1']
                     Note_Scale_PR = 'Low'
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     Col_PR = '#F8E71C'  # yellow
                     Note_PR = 'Precipitation Level exceeds 1st alarm threshold '
                     #Scale_PR = 1 #['1']
                     Scale_PR = 2 #['2']
                     Note_Scale_PR = 'Medium'
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     Col_PR = '#E68431'  # orange
                     Note_PR = 'Precipitation Level exceeds 2nd alarm threshold '
                     #Scale_PR = 2 #['2']
                     Scale_PR = 3 #['3']
                     Note_Scale_PR = 'High'
                 else:  # value >= item['Alarm3']:
                     Col_PR = '#AA2050' # red
                     Note_PR = 'Precipitation Level exceeds 3rd alarm threshold '
                     #Scale_PR = 3 #['3']
                     Scale_PR = 4 #['4']
                     Note_Scale_PR = 'Very High'


        MColNote_PR = [Col_PR, Note_PR, Scale_PR, Note_Scale_PR]

        return MColNote_PR


#---------------------------------------------------------------------------------------
#
# Calculate the Crisis Classification Level for each River Section
#   If the value exceeds one of the predefined thresholds then
#   the corresponding note, color and scale will be assigned to it and
#   the flag will be set as true. If flag_extreme is equal to false then the
#   value does not belong to any case.
#
def compare_forecast_new_scale_thresholds(value, Thresholds):

    flag_extreme = False

    # Compare maximum observation of the Obs_yv with the Thresholds
    if value >= Thresholds[0] and value < Thresholds[1]:
        meas_color = ['#F8E71C']  # yellow
        meas_note = ['Water level overflow: exceeding of the 1st alarm threshold']
        scale = 2
        scale_note = ["Medium"]
        flag_extreme = True
    elif value >= Thresholds[1] and value < Thresholds[2]:
        meas_color = ['#E68431']  # orange
        meas_note = ['Water level overflow: exceeding of the 2nd alarm Thresholds']
        scale = 3
        scale_note = ["High"]
        flag_extreme = True
    elif value >= Thresholds[2]:
        meas_color = ['#AA2050']  # red
        meas_note = ['Water level overflow: exceeding of the 3rd alarm threshold']
        scale = 4
        scale_note = ["Very High"]
        flag_extreme = True
    else:
        meas_color = ['#31A34F']  # green
        meas_note = ['Water level OK']
        scale = 1
        scale_note = ["Low"]
        flag_extreme = True

    if flag_extreme == True:
        resp_comp = [meas_color, meas_note, scale, scale_note, flag_extreme]
    else:
        resp_comp = ["None", "None", "None", "None", flag_extreme]

    return resp_comp

#-------------------- OBSOLETE SCALE {0,...,3}

def compare_forecast_scale_thresholds(value, Thresholds):

    flag_extreme = False

    # Compare maximum observation of the Obs_yv with the Thresholds
    if value >= Thresholds[0] and value < Thresholds[1]:
         meas_color = ['#F8E71C']  # yellow
         meas_note = ['Water level overflow: exceeding of the 1st alarm threshold']
         scale = 1
         scale_note = ["Medium"]
         flag_extreme = True
    elif value >= Thresholds[1] and value < Thresholds[2]:
         meas_color = ['#E68431']  # orange
         meas_note = ['Water level overflow: exceeding of the 2nd alarm Thresholds']
         scale = 2
         scale_note = ["High"]
         flag_extreme = True
    elif value >= Thresholds[2]:
         meas_color = ['#AA2050']  # red
         meas_note = ['Water level overflow: exceeding of the 3rd alarm threshold']
         scale = 3
         scale_note = ["Very High"]
         flag_extreme = True
    else:
         meas_color = ['#31A34F']  # green
         meas_note = ['Water level OK']
         scale = 0
         scale_note = ["Low"]
         flag_extreme = True

    if flag_extreme == True:
         resp_comp = [meas_color, meas_note, scale, scale_note, flag_extreme]
    else:
         resp_comp = ["None", "None", "None", "None", flag_extreme]

    return resp_comp

#--------------------------------------------------------------------
# Compate the values in a list with the pre-defined thresholds
#
def compare_forecasts_df_thresholds_PILOT(df_value, Thresholds):

    new_df = pd.DataFrame([])

    for it in range(len(df_value)):

        flag_extreme = False
        value = df_value.iloc[it]['result']

        # Compare maximum observation of the Obs_yv with the Thresholds
        if value >= Thresholds[0] and value < Thresholds[1]:
            meas_color = ['#F8E71C']  # yellow
            meas_note = ['Water level overflow: exceeding of the 1st alarm threshold']
            scale = 2
            scale_note = ["Medium"]
            flag_extreme = True
        elif value >= Thresholds[1] and value < Thresholds[2]:
            meas_color = ['#E68431']  # orange
            meas_note = ['Water level overflow: exceeding of the 2nd alarm Thresholds']
            scale = 3
            scale_note = ["High"]
            flag_extreme = True
        elif value >= Thresholds[2]:
            meas_color = ['#AA2050']  # red
            meas_note = ['Water level overflow: exceeding of the 3rd alarm threshold']
            scale = 4
            scale_note = ["Very High"]
            flag_extreme = True
        else:
            meas_color = ['#31A34F']  # green
            meas_note = ['Water level OK']
            scale = 1
            scale_note = ["Low"]
            flag_extreme = True

        data = {"Measurement_Color": meas_color, "Measurement_Note": meas_note,
                "Scale": scale, "Scale_Note": scale_note, "Flag_Extreme": flag_extreme}

        resp_comp = pd.DataFrame(data, columns=["Measurement_Color", "Measurement_Note", "Scale", "Scale_Note", "Flag_Extreme"])

        new_df = pd.concat([new_df, resp_comp], ignore_index=True)

    # Concatenate data frames
    new_df = pd.concat([df_value, new_df], axis=1, ignore_index=True)

    new_df.columns = ['RS_Name', 'RS_ID', 'phenomenonTime', 'result', 'Lat', 'Long', "Measurement_Color",
                      "Measurement_Note", "Scale", "Scale_Note", "Flag_Extreme"]

    return new_df



#--------------------------------------------------------------------------------------------------
# Calculates the generalized (power) mean
#
def generalized_mean(freqs, scale, p):

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

#--------------------------------------------------------------------------------------------------
# Calculates the Overall Crisis Classification Index per group of river sections and total
#
#   Input: a list of dictionaries, each one corresponds to each group of river sections and
#           contains a vector with frequencies of each scale 'count':[n0, n1, n2, n3]
#
#   flag_scale = TRUE -> new scale is used {1,2,3,4}, otherwise the old scale is used {0,1,2,3}
#
#   Output: a list of Overall_Crisis_Classification_Index per group of river sections and total
#
def Overall_Crisis_Classification_Index( RiverSect_CountScale, flag_scale ):

    OCCI = []
    total = [0,0,0,0]

    # find the maximum scale for each one of the group of river sections
    for it in range( len(RiverSect_CountScale) ):

        counts = RiverSect_CountScale[it]['count']

        scale = [1,2,3,4]
        p4 = 4
        quatr_mean = generalized_mean(counts, scale, p4)

        p3 = 3
        cubic_mean = generalized_mean(counts, scale, p3)

       # print("quatr_mean = ", quatr_mean, " ||| cubic_mean =", cubic_mean)

        occi_val = ceil(quatr_mean)
        occi_val_3 = ceil(cubic_mean)

        if occi_val == 1:
            col = '#31A34F'  # green
            note = 'Low'
        elif occi_val == 2:
            col = '#F8E71C'  # yellow
            note = "Medium"
        elif occi_val == 3:
            col = '#E68431'  # orange
            note = "High"
        else:
            col = '#AA2050'  # red
            note = "Very High"

        OCCI += [ {'name': RiverSect_CountScale[it]['name'],
                   'occi': occi_val,
                   'color': col,
                   'note': note} ]

        # Update total
        if len(total) != 0:
            total = [sum(total) for total in zip(total, counts)]
        else:
            total = counts

    # Calculate the total Overall Crisis Classification Index
    scale = [1,2,3,4]
    p4 = 4
    tot_quatr_mean = generalized_mean(total, scale, p4)

    p3 = 3
    tot_cubic_mean = generalized_mean(total, scale, p3)

#    print("total quatr_mean = ", tot_quatr_mean, " ||| total cubic_mean =", tot_cubic_mean)

    tot_occi_val = ceil(tot_quatr_mean)
    tot_occi_val_3 = ceil(tot_cubic_mean)

    if tot_occi_val == 1:
        col = '#31A34F' # green
        note = 'Low'
    elif tot_occi_val == 2:
        col = '#F8E71C'  # yellow
        note = "Medium"
    elif tot_occi_val == 3:
        col = '#E68431'  # orange
        note = "High"
    else:
        col = '#AA2050'  # red
        note = "Very High"

    OCCI += [ {'name': 'TOTAL', 'occi': tot_occi_val, 'color': col, 'note': note} ]

    return(OCCI)

#--------------------------------------------------------------------------------------------------
# Calculates the Crisis Levels for every group of River Sections and over all river sections
# in the region of interest
#
#  Update version for v9
#
#   Inputs: RiverSect_CountScale - list of dictionaries
#            Overall Crisis Classification Index - list of dictionaries
#
#   Output: Crisis Lever per group and overall (global)
#
def Group_Overall_Crisis_Level(RSCS, occi, weights):

    len_rscs = len(RSCS)
    total = [0,0,0,0]
    if len_rscs > 1:
        for it in range( len_rscs ):
            counts = RSCS[it]['count']
            # Update total
            if len(total) != 0:
                total = [sum(total) for total in zip(total, counts)]
            else:
                total = counts
    else:
        total = RSCS[0]['count']

    # list of Overall Crisis Level indices per group and global
    OCL = []
    len_occi = len(occi)
    for i in range(len_rscs):
        # Calculate OCL for each group
        if occi[i]['name'] == RSCS[i]['name']:
           group_occi = occi[i]['occi']

           group_count = RSCS[i]['count']
           n4 = group_count[len(group_count)-1]
           name_text = RSCS[i]['name']

           if group_occi == 1:
                note_text = 'Low with ' + str(n4) + ' river sections of scale 4'
                temp_OCL = { 'name': name_text, 'category': 'Low', 'note': note_text, 'color': '#31A34F',
                             'ocl': str(group_occi), 'ocl_val':str(group_occi/4*100 - 12.5) }

           elif group_occi == 2 and n4 == 0:
                note_text = 'Medium with ' + str(n4) + ' river sections of scale 4'
                temp_OCL = { 'name': name_text, 'category': 'Medium', 'note': note_text, 'color': '#F8E71C',
                             'ocl': str(group_occi), 'ocl_val':str(group_occi/4*100 - 12.5) }

           elif group_occi == 2 and n4 != 0:
                note_text = 'Medium with ' + str(n4) + ' river sections of scale 4'
                #color gold
                temp_OCL = { 'name': name_text, 'category': 'Medium', 'note': note_text, 'color': '#FFD700', 'ocl': str(group_occi) + "+",
                             'ocl_val':str((group_occi+0.5)/4*100 - 12.5) }

           elif group_occi == 3 and n4 == 0:
                note_text = 'High with ' + str(n4) + ' river sections of scale 4'
                temp_OCL = { 'name': name_text, 'category': 'High', 'note': note_text, 'color': '#E68431', 'ocl': str(group_occi),
                             'ocl_val':str(group_occi/4*100 - 12.5)}

           elif group_occi == 3 and n4 != 0:
                note_text = 'High with ' + str(n4) + ' river sections of scale 4'
                #color dark orange
                temp_OCL = { 'name': name_text, 'category': 'High', 'note': note_text, 'color': '#EE7600', 'ocl': str(group_occi)+"+",
                             'ocl_val':str((group_occi+0.5)/4*100 - 12.5) }
           else:
                note_text = 'Very High with ' + str(n4) + ' river sections of scale 4'
                temp_OCL = { 'name': name_text, 'category': 'Very High', 'note': note_text, 'color': '#AA2050', 'ocl': str(group_occi),
                             'ocl_val':str(group_occi/4*100 - 12.5) }

           OCL += [temp_OCL]
           print(OCL[i])

    # Calculate OCL for the whole Region of Interest (global)

    wavg = weighted_avg(OCL, weights)

    if occi[len_occi-1]['name'] == 'TOTAL':

        total_occi = occi[len_occi-1]['occi']

        if total_occi == wavg:
            print("\n Weighted avg is equal with the total_occi \n")
            print("total_occi =", total_occi, " and Wavg = ", wavg)
        else:
            print("\n Weighted avg is NOT equal with the total_occi \n")
            print("total_occi =", total_occi, " and Wavg = ", wavg)
            total_occi = wavg

        n4 = total[len(total)-1]
        name_text = 'All rivers in the Municipality'

        if total_occi == 1:
           note_text = 'Low with ' + str(n4) + ' river sections of scale 4'
           temp_OCL = { 'name': name_text, 'category': 'Low', 'note': note_text, 'color': '#31A34F', 'ocl': str(total_occi),
                        'ocl_val':str(total_occi/4*100 - 12.5) }
        elif total_occi == 2 and n4 == 0:
           note_text = 'Medium with ' + str(n4) + ' river sections of scale 4'
           temp_OCL = { 'name': name_text, 'category': 'Medium', 'note': note_text, 'color': '#F8E71C', 'ocl': str(total_occi),
                        'ocl_val':str(total_occi/4*100 - 12.5) }
        elif total_occi == 2 and n4 != 0:
           note_text = 'Medium with ' + str(n4) + ' river sections of scale 4'
           #color gold
           temp_OCL = { 'name': name_text, 'category': 'Medium', 'note': note_text, 'color': '#FFD700', 'ocl': str(total_occi)+"+",
                        'ocl_val':str((total_occi+0.5)/4*100 - 12.5) }
        elif total_occi == 3 and n4 == 0:
           note_text = 'High with ' + str(n4) + ' river sections of scale 4'
           temp_OCL = { 'name': name_text, 'category': 'High', 'note': note_text,  'color': '#E68431', 'ocl': str(total_occi),
                        'ocl_val':str(total_occi/4*100 -12.5) }
        elif total_occi == 3 and n4 != 0:
           note_text = 'High with ' + str(n4) + ' river sections of scale 4'
           #color dark orange
           temp_OCL = { 'name': name_text, 'category': 'High', 'note': note_text, 'color': '#EE7600', 'ocl': str(total_occi)+"+",
                        'ocl_val':str((total_occi+0.5)/4*100 - 12.5) }
        else:
           note_text = 'Very High with ' + str(n4) + ' river sections of scale 4'
           temp_OCL = { 'name': name_text, 'category': 'Very High', 'note': note_text, 'color': '#AA2050', 'ocl': str(total_occi),
                        'ocl_val':str(total_occi/4*100 - 12.5) }

        temp_OCL.update({'ocl_val' : total_occi/4*100 - 12.5})
        OCL += [temp_OCL]

    return(OCL)

#--------------------------------------------------------------------------------------------------
# Calculates the weighted average over the Overall Crisis Level for each group of River Sections
#   Inputs the OCL values per group and a vector of weights. Default weight values are equal to 1
#
def weighted_avg(OCL, weights):

   ocl_values = []
   for it in range(len(OCL)):
       ocl_values.append( float( OCL[it]['ocl'].split("+")[0] ) )

   wa = ceil(sum(x * y for x, y in zip(ocl_values, weights)) / sum(weights))

   return(wa)


#----------------------------------------------------------------------------------------------------
# Calculates the Overall Crisis Level over all Weather Stations
#
#   Input: a list of dictionaries, each one corresponds to each group of river sections and
#           contains a vector with frequencies of each scale 'count':[n0, n1, n2, n3]
#
#   Output: a total value of Overall_Crisis_Level index for all Weather Stations
#
def Overall_Crisis_Level_WeatherStations( meas_ColNote_WL ):

    scale = [1,2,3,4]
    counts = [0,0,0,0]

    # name_text = 'Overall Crisis Level for all Weather Stations at Region Of Interest'
    name_text = 'All Weather Stations in Municipality'

    for i in range(len(meas_ColNote_WL)):
        ps = meas_ColNote_WL[i]['scale']
        if ps != None:
            counts[ps-1] += 1

            p4 = 4
            tot_quatr_mean = generalized_mean(counts, scale, p4)

            total_occi_val = ceil(tot_quatr_mean)

            n4 = counts[len(counts) - 1]

            if total_occi_val == 1:
                OCL = {'name': name_text, 'note': 'Low', 'color': '#31A34F', 'ocl': str(total_occi_val),
                       'ocl_val':str(total_occi_val/4*100 - 12.5) }
            elif total_occi_val == 2 and n4 == 0:
                OCL = {'name': name_text, 'note': 'Medium', 'color': '#F8E71C', 'ocl': str(total_occi_val),
                       'ocl_val':str(total_occi_val/4*100 - 12.5)}
            elif total_occi_val == 2 and n4 != 0:
                note_text = 'Medium with ' + str(n4) + ' weather stations of scale 4'
                # color gold
                OCL = {'name': name_text, 'note': note_text, 'color': '#FFD700', 'ocl': str(total_occi_val) + "+",
                       'ocl_val':str((total_occi_val+0.5)/4*100 - 12.5)}
            elif total_occi_val == 3 and n4 == 0:
                OCL = {'name': name_text, 'note': 'High', 'color': '#E68431', 'ocl': str(total_occi_val),
                       'ocl_val':str(total_occi_val/4*100 - 12.5)}
            elif total_occi_val == 3 and n4 != 0:
                note_text = 'High with ' + str(n4) + ' river sections of scale 4'
                # color dark orange
                OCL = {'name': name_text, 'note': note_text, 'color': '#EE7600',
                       'ocl': str(total_occi_val) + "+", 'ocl_val':str((total_occi_val+0.5)/4*100 - 12.5)}
            else:
                OCL = {'name': name_text, 'note': 'Very High', 'color': '#AA2050', 'ocl': str(total_occi_val),
                       'ocl_val':str(total_occi_val/4*100 - 12.5)}
        else:
            OCL = {'name': name_text, 'note': None, 'color': None, 'ocl': None, 'ocl_val': None}

    return (OCL)

#-------------------------------------------------------------------------------------
# Function which maps the River Section ID with SensorID and new ID for Topic104
#
def mappingRS(riverSections):

    df = pd.DataFrame(
        columns=['SensorID', 'DataSeriesID', 'DataSeriesName', 'Treshold1', 'Treshold2', 'Treshold3', 'Lat', 'Long'])

    SensorID = []
    DataSeriesID = []
    DataSeriesName = []
    Treshold1 = []
    Treshold2 = []
    Treshold3 = []
    Lat = []
    Long = []

    for i in range(len(riverSections['value'])):
        #id_str = '@iot.id: '
        DataSeries_str = 'RS' + '_'
        #SensorID.append(id_str + str(riverSections['value'][i]['@iot.id']))
        SensorID.append(riverSections['value'][i]['@iot.id'])
        DataSeriesID.append(DataSeries_str + str( round(riverSections['value'][i]['properties']['distance'], 2) ))
        Treshold1.append(riverSections['value'][i]['properties']['treshold1'])
        Treshold2.append(riverSections['value'][i]['properties']['treshold2'])
        Treshold3.append(riverSections['value'][i]['properties']['treshold3'])
        Long.append(riverSections['value'][i]['Locations'][0]['location']['coordinates'][0])
        Lat.append(riverSections['value'][i]['Locations'][0]['location']['coordinates'][1])
        DataSeriesName.append(riverSections['value'][i]['name'])

    df['SensorID'] = SensorID
    df['DataSeriesID'] = DataSeriesID
    df['DataSeriesName'] = DataSeriesName
    df['Treshold1'] = Treshold1
    df['Treshold2'] = Treshold2
    df['Treshold3'] = Treshold3
    df['Lat'] = Lat
    df['Long'] = Long

    return df

#-------------------------------------------------------------------------------------
# Function which maps the Weather Stations ID with SensorID and new ID for Topic104
#
def mappingWS(station, directory):
    df = pd.DataFrame(columns=['SensorID', 'DataSeriesID', 'DataSeriesName', 'Lat', 'Long'])
    SensorID = []
    DataSeriesID = []
    DataSeriesName = []
    Lat = []
    Long = []
    for i in range(len(station['value'])):
    #    id_str = '@iot.id: '
        DataSeries_str = 'WS' + "_"
    #    SensorID.append(id_str + str(station['value'][i]['@iot.id']))
        SensorID.append( station['value'][i]['@iot.id'] )
        DataSeriesID.append(DataSeries_str + str(station['value'][i]['@iot.id']))
        Long.append(station['value'][i]['Locations'][0]['location']['coordinates'][0])
        Lat.append(station['value'][i]['Locations'][0]['location']['coordinates'][1])
        DataSeriesName.append(station['value'][i]['name'])

    df['SensorID'] = SensorID
    df['DataSeriesID'] = DataSeriesID
    df['DataSeriesName'] = DataSeriesName
    df['Lat'] = Lat
    df['Long'] = Long

    return df


#========================================================================================
#
def extract_features(response_q):
    features_list = []

    for i in range(len(response_q['features'])):
        temp_dict = {}
        temp_dict.update({'ID': response_q['features'][i]['id']})
        temp_dict.update({'Type': response_q['features'][i]['geometry']['type']})
        temp_dict.update({'Coordinates': response_q['features'][i]['geometry']['coordinates'][0][0]})
        temp_dict.update({'RiskValue': response_q['features'][i]['properties']['VALORE']})
        temp_dict.update({'RiskClass': response_q['features'][i]['properties']['CLASSE']})
        temp_dict.update({'Hazard': response_q['features'][i]['properties']['PERICOLO']})
        temp_dict.update({'Damage': response_q['features'][i]['properties']['DANNO']})

        # Extract the severity and its color from CLASSE property
        if response_q['features'][i]['properties']['CLASSE'] == 'R1':
            temp_dict.update({'Severity': 'Low'})
            temp_dict.update({'Color': {'r': 0, 'g': 0, 'b': 255}})
        elif response_q['features'][i]['properties']['CLASSE'] == 'R2':
            temp_dict.update({'Severity': 'Medium'})
            temp_dict.update({'Color': {'r': 255, 'g':0, 'b': 255}})
        elif response_q['features'][i]['properties']['CLASSE'] == 'R3':
            temp_dict.update({'Severity': 'High'})
            temp_dict.update({'Color': {'r': 255, 'g':0, 'b': 128}})
        elif response_q['features'][i]['properties']['CLASSE'] == 'R4':
            temp_dict.update({'Severity': 'Very High'})
            temp_dict.update({'Color': {'r': 255, 'g':0, 'b': 0}})

        # cand_class = ['R3', 'R4']
        # cand_class = ['R2', 'R3', 'R4']
        cand_class = ['R1', 'R2', 'R3', 'R4']

        if response_q['features'][i]['properties']['CLASSE'] in cand_class:
            features_list.append(temp_dict)

    return(features_list)

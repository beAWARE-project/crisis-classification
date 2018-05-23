def compare_value_thresholds(value, StID, dstr_ind, Thresholds):

    StationID = StID[0]
    dstr_indicator = dstr_ind[0]

    if dstr_indicator == 'Water':
        # find the element of the Thresholds (_WL dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):

            if StationID == str(item['ID']):
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     MCol_WL = ['#00FF00']  # green
                     MNote_WL = ['Water Level OK - Moderate Crisis Level']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_WL = ['#FFFF00']  # yellow
                     MNote_WL = ['Water Level exceeds 1st alarm threshold - Medium Crisis Level']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_WL = ['#FFA500']  # orange
                     MNote_WL = ['Water Level exceeds 2nd alarm threshold - High Crisis Level']
                 else:  # value >= item['Alarm3']:
                     MCol_WL = ['#FF0000'] # red
                     MNote_WL = ['Water Level exceeds 3rd alarm threshold - Very High Crisis Level']


        MColNote_WL = [MCol_WL, MNote_WL]

        return MColNote_WL

    if dstr_indicator == 'Precipitation':
        # find the element of the Thresholds (_PR dictionary) which corresponds to the StationID
        for iter, item in enumerate(Thresholds):
            if StationID == str(item['ID']):
                 # compare the thresholds with the value
                 if value < item['Alarm1']:
                     MCol_PR = ['#00FF00']  # green
                     MNote_PR = ['Precipitation Level OK - Moderate Crisis Level']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_PR = ['#FFFF00']  # yellow
                     MNote_PR = ['Precipitation Level exceeds 1st alarm threshold - Medium Crisis Level']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_PR = ['#FFA500']  # orange
                     MNote_PR = ['Precipitation Level exceeds 2nd alarm threshold - High Crisis Level']
                 else:  # value >= item['Alarm3']:
                     MCol_PR = ['#FF0000'] # red
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
                     MCol_WL = ['#00FF00']  # green
                     MNote_WL = ['Water Level OK - Moderate Crisis Level']
                     #Scale_WL = 0 #['0']
                     Scale_WL = 1 #['1']
                     Note_Scale_WL = ['Low']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_WL = ['#FFFF00']  # yellow
                     MNote_WL = ['Water Level exceeds 1st alarm threshold - Medium Crisis Level']
                     #Scale_WL = 1 #['1']
                     Scale_WL = 2 #['2']
                     Note_Scale_WL = ['Medium']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_WL = ['#FFA500']  # orange
                     MNote_WL = ['Water Level exceeds 2nd alarm threshold - High Crisis Level']
                     #Scale_WL = 2 #['2']
                     Scale_WL = 3 #['3']
                     Note_Scale_WL = ['High']
                 else:  # value >= item['Alarm3']:
                     MCol_WL = ['#FF0000'] # red
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
                     MCol_PR = ['#00FF00']  # green
                     MNote_PR = ['Precipitation Level OK - Moderate Crisis Level']
                     #Scale_PR = 0 #['0']
                     Scale_PR = 1 #['1']
                     Note_Scale_PR = ['Low']
                 elif value >= item['Alarm1'] and value < item['Alarm2']:
                     MCol_PR = ['#FFFF00']  # yellow
                     MNote_PR = ['Precipitation Level exceeds 1st alarm threshold - Medium Crisis Level']
                     #Scale_PR = 1 #['1']
                     Scale_PR = 2 #['2']
                     Note_Scale_PR = ['Medium']
                 elif value >= item['Alarm2'] and value < item['Alarm3']:
                     MCol_PR = ['#FFA500']  # orange
                     MNote_PR = ['Precipitation Level exceeds 2nd alarm threshold - High Crisis Level']
                     #Scale_PR = 2 #['2']
                     Scale_PR = 3 #['3']
                     Note_Scale_PR = ['High']
                 else:  # value >= item['Alarm3']:
                     MCol_PR = ['#FF0000'] # red
                     MNote_PR = ['Precipitation Level exceeds 3rd alarm threshold - Very High Crisis Level']
                     #Scale_PR = 3 #['3']
                     Scale_PR = 4 #['4']
                     Note_Scale_PR = ['Very High']

        MColNote_PR = [MCol_PR, MNote_PR, Scale_PR, Note_Scale_PR]

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
        meas_color = ['#FFFF00']  # yellow
        meas_note = ['Water level overflow: exceeding of the 1st alarm threshold']
        scale = 2
        scale_note = ["Medium"]
        flag_extreme = True
    elif value >= Thresholds[1] and value < Thresholds[2]:
        meas_color = ['#FFA500']  # orange
        meas_note = ['Water level overflow: exceeding of the 2nd alarm Thresholds']
        scale = 3
        scale_note = ["High"]
        flag_extreme = True
    elif value >= Thresholds[2]:
        meas_color = ['#FF0000']  # red
        meas_note = ['Water level overflow: exceeding of the 3rd alarm threshold']
        scale = 4
        scale_note = ["Very High"]
        flag_extreme = True
    else:
        meas_color = ['#00FF00']  # green
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
         meas_color = ['#FFFF00']  # yellow
         meas_note = ['Water level overflow: exceeding of the 1st alarm threshold']
         scale = 1
         scale_note = ["Medium"]
         flag_extreme = True
    elif value >= Thresholds[1] and value < Thresholds[2]:
         meas_color = ['#FFA500']  # orange
         meas_note = ['Water level overflow: exceeding of the 2nd alarm Thresholds']
         scale = 2
         scale_note = ["High"]
         flag_extreme = True
    elif value >= Thresholds[2]:
         meas_color = ['#FF0000']  # red
         meas_note = ['Water level overflow: exceeding of the 3rd alarm threshold']
         scale = 3
         scale_note = ["Very High"]
         flag_extreme = True
    else:
         meas_color = ['#00FF00']  # green
         meas_note = ['Water level OK']
         scale = 0
         scale_note = ["Low"]
         flag_extreme = True

    if flag_extreme == True:
         resp_comp = [meas_color, meas_note, scale, scale_note, flag_extreme]
    else:
         resp_comp = ["None", "None", "None", "None", flag_extreme]

    return resp_comp

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

    from math import ceil

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

        print("quatr_mean = ", quatr_mean, " ||| cubic_mean =", cubic_mean)

        occi_val = ceil(quatr_mean)
        occi_val_3 = ceil(cubic_mean)

        if occi_val == 1:
            col = '#00FF00'  # green
            note = 'Low'
        elif occi_val == 2:
            col = '#FFFF00'  # yellow
            note = "Medium"
        elif occi_val == 3:
            col = '#FFA500'  # orange
            note = "High"
        else:
            col = '#FF0000'  # red
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

    print("total quatr_mean = ", tot_quatr_mean, " ||| total cubic_mean =", tot_cubic_mean)

    tot_occi_val = ceil(tot_quatr_mean)
    tot_occi_val_3 = ceil(tot_cubic_mean)

    if tot_occi_val == 1:
        col = '#00FF00' # green
        note = 'Low'
    elif tot_occi_val == 2:
        col = '#FFFF00'  # yellow
        note = "Medium"
    elif tot_occi_val == 3:
        col = '#FFA500'  # orange
        note = "High"
    else:
        col = '#FF0000'  # red
        note = "Very High"

    OCCI += [ {'name': 'TOTAL', 'occi': tot_occi_val, 'color': col, 'note': note} ]

    return(OCCI)

#--------------------------------------------------------------------------------------
# Calculates the Overall Crisis Classification Index over all Weather Stations
#
#   Input: a list of dictionaries, each one corresponds to each group of river sections and
#           contains a vector with frequencies of each scale 'count':[n0, n1, n2, n3]
#
#   Output: a total value of Overall_Crisis_Classification_Index
#
def Overall_Crisis_Classification_Index_WeatherStations( meas_ColNote_WL ):

    from math import ceil

    scale = [1,2,3,4]
    counts = [0,0,0,0]

    for i in range(len(meas_ColNote_WL)):
        ps = meas_ColNote_WL[i]['scale']
        counts[ps-1] += 1

    p4 = 4
    tot_quatr_mean = generalized_mean(counts, scale, p4)

    tot_occi_val = ceil(tot_quatr_mean)

    if tot_occi_val == 1:
        col = '#00FF00' # green
        note = 'Low'
    elif tot_occi_val == 2:
        col = '#FFFF00'  # yellow
        note = "Medium"
    elif tot_occi_val == 3:
        col = '#FFA500'  # orange
        note = "High"
    else:
        col = '#FF0000'  # red
        note = "Very High"

    OCCI = [ {'name': 'TOTAL', 'occi': tot_occi_val, 'color': col, 'note': note} ]

    return(OCCI)


#--------------------------------------------------------------------------------------------------
# Calculates the Overall Crisis Level over all river sections in the region of interest
#
#   Inputs: RiverSect_CountScale - list of dictionaries
#            Overall Crisis Classification Index - list of dictionaries
def Overall_Crisis_Level(RSCS, occi):

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

    len_occi = len(occi)
    for i in range(len_occi):
        if occi[i]['name'] == 'TOTAL':
            total_occi = occi[i]['occi']

    n4 = total[len(total)-1]
    name_text = 'Overall Crisis Level for all ROI'

    if total_occi == 1:
        OCL = { 'name': name_text, 'note': 'Low', 'color': '#00FF00', 'ocl': str(total_occi)}
    elif total_occi == 2 and n4 == 0:
        OCL = { 'name': name_text, 'note': 'Medium', 'color': '#FFFF00', 'ocl': str(total_occi)}
    elif total_occi == 2 and n4 != 0:
        note_text = 'Medium with ' + str(n4) + ' river sections of scale 4'
        #color gold
        OCL = { 'name': name_text, 'note': note_text, 'color': '#FFD700', 'ocl': str(total_occi)+"+"}
    elif total_occi == 3 and n4 == 0:
        OCL = { 'note': 'High', 'color': '#FFA500', 'ocl': str(total_occi)}
    elif total_occi == 3 and n4 != 0:
        note_text = 'High with ' + str(n4) + ' river sections of scale 4'
        #color dark orange
        OCL = { 'name': name_text, 'note': note_text, 'color': '#EE7600', 'ocl': str(total_occi)+"+"}
    else:
        OCL = { 'name': name_text, 'note': 'Very High', 'color': '#FF0000', 'ocl': str(total_occi)}

    return(OCL)

#--------------------------------------------------------------------------------------------------
# Calculates the hazard based on the value of hazard field (hf_val) in the APP
# Output: 0 <= Hazard <= 1
#
def calculate_hazard(hf_val):

    if hf_val < 0.5 :
        Hazard = {'Value': 0.4, 'Description': 'Low hazard'}
    elif 0.5 <= hf_val <= 1:
        Hazard = {'Value': 0.8, 'Description': 'Medium hazard'}
    else:
        Hazard = {'Value': 1.0, 'Description': 'High hazard'}

    return(Hazard)

#--------------------------------------------------------------------------------------------------
# Calculates the Vulnerability based on the value of WL and element_at_risk (exposure field)
# coming from the APP
#
# Output: Vulnerability which consists of Vp (vulnerability of people),
#            Ve (vulnerability of economic activities)
#        and Va (vulnerability of environments and cultural assets)
#
#calculate_vulnerability( WL, element_at_risk ):

#    if len()

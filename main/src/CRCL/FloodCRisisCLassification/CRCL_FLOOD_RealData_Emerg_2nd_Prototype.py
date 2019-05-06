# Created Date: 04/03/2019
# Modified Date: 05/04/2019
#
# -------------------------------- 2nd DEVELOPMENT PHASE - REAL-TIME OBSERVATIONS -------------------------------------------
#
#
# Implements the 2nd algorithm of Crisis Classification module
# based on the measurements of water levels from sensors for a
# specific 3 weather stations at a) last measurement
# or b) at particular date/time period.
# If flag_phenTime = True, then the phenomenonTime of each Weather Station will be considered,
# otherwise the specific dates/time period will be examined
#
#----------------------------------------------------------------------------------------------------------
# Inputs: a) Time series of measurements of water levels by the sensors
#               for a specific weather station at last measurement (date/time)
#         b) Thresholds for the particular specific weather station and for water river level
#
# Outputs: TOP104_METRIC_REPORT for each Weather Station and Datastream, which contains:
#         a) the actual crisis level associated to the sensor position
#         b) a metric-scale from 0 to 3 depending on whether the actual value exceeds a
#            particular alarm threshold.
#
#   Algorithm 2 from Crisis Classification (based on AAWA)  Ver 2
#----------------------------------------------------------------------------------------------------------
#

from bus.bus_producer import BusProducer
import json, time
from datetime import datetime, timedelta
import os, errno
import numpy as np
from pathlib import Path
from collections import OrderedDict

import pandas as pd

from CRCL.FloodCRisisCLassification.Topic104_Metric_Report import Top104_Metric_Report
from CRCL.FloodCRisisCLassification.topic104_Flood_Emerg import *

from CRCL.FloodCRisisCLassification.Topic006_Incident_Report_CRLC import Top006_Incident_Report_CRCL
from CRCL.FloodCRisisCLassification.topic006_Flood_Emerg_IncidentReports_CRCL import *

from CRCL.FloodCRisisCLassification.Create_Queries import *
from CRCL.FloodCRisisCLassification.Auxiliary_functions import *

from CRCL.FloodCRisisCLassification.Topic007_Update_Incident_Risk import *
from CRCL.FloodCRisisCLassification.topic007_Flood_Emerg_Update_Incident_Risk import *


def CrisisClassificationFlood_RealData_Emerg_v2(cur_date, flag_mode):

    ver = 'RealData_Emerg_2nd_PROTOTYPE'

    count_top104_WL = 0
    count_top104_PR = 0
    count_top104_WL_Map = 0
    count_top104_PR_Map = 0
    count_top006_WL = 0
    count_top006_PR = 0
    count_top104_OCL = 0

    # Number of observations that requested (Top parameter)
    Num_Interest_Obs = 24

    # Create a directory to store the output files and TOPICS
    # Create a path
    current_dirs_parent = os.getcwd()
    root_path = current_dirs_parent + "/" + "CRCL/FloodCRisisCLassification" + "/"

    now = cur_date.isoformat()
    now_WL = str(now)+'Z'
    now_PR = str(now)+'Z'
    directory = root_path + ver + "_" + "TOPICS" + "_" + flag_mode + "_" + now
    os.makedirs(directory, exist_ok=True)

    #-----------------------------------------------------------------------------------
    # Fetch data from the OGC SensorThings API
    #
    # User defined values in order to formulate the query

    service_root_URI = 'https://beaware.server.de/SensorThingsService/v1.0/'

    SensorThingEntities = ['Things', 'Locations', 'HistoricalLocations',
                            'Datastreams', 'Sensor', 'Observations',
                            'ObservedProperties', 'FeaturesOfInterest', 'MultiDatastreams']

    SensorThings = [SensorThingEntities[0], SensorThingEntities[3], SensorThingEntities[5]]

    # Initialise arrays to store the results of comparison for each weather station and each datastream (WL or PR)
    meas_ColNote_WL = []
    meas_ColNote_PR = []

    # --------------------------------------------------------------------------------------
    # Creates the thresholds for each one of the Weather Stations of interest
    # ID =  ID from SensorThingServer, Alarms= Threshold, ID_DS = IDs for DataStreams Observations

    Weather_Stations_Names_Ids = [ {'Name': 'Valli del Pasubio', 'ID': 29},
                                   {'Name': 'Timonchio a Ponte Marchese CAE', 'ID': 45},
                                   {'Name': 'Bacchiglione a Vicenza CAE', 'ID': 47},
                                   {'Name': 'Vicenza S.Agostino', 'ID': 49},
                                   { 'Name': 'Retrone a Vicenza S.Agostino CAE', 'ID': 374} ]

    # Weather_Stations_Ids = [29, 45, 47, 49, 374]

    Thresholds_WL = [{'ID': 45, 'Alarm1': 1.00, 'Alarm2': 2.20, 'Alarm3': 3.20, 'ID_DS': 165},
                     {'ID': 47, 'Alarm1': 3.00, 'Alarm2': 4.60, 'Alarm3': 5.40, 'ID_DS': 167},
                     {'ID': 374, 'Alarm1': 1.00, 'Alarm2': 2.40, 'Alarm3': 2.80, 'ID_DS': 492}
                     ]

    Thresholds_PR = [{'ID': 29, 'Alarm1': 50, 'Alarm2': 100, 'Alarm3': 150, 'ID_DS': 140},
                     {'ID': 49, 'Alarm1': 50, 'Alarm2': 100, 'Alarm3': 150, 'ID_DS': 157}
                    ]

    # Start Timing Step 1
    start_step1 = time.time()

    # Store the time steps
    time_duration_step = []

    #-----------------------------------------------------------------------------------------------------------
    # Steps : 1) Extracts the weather stations where have as Datastreams the Water Level and Precipitation
    #         2) For each WS, calculates the scale of WL and Precipitation regarding the thresholds exceeding
    #         3) Create Topics

    producer = BusProducer()

    # Decorate terminal
    print('\033[95m' + "\n***********************")
    print("*** CRCL SERVICE v1.0 ***")
    print("***********************\n" + '\033[0m')

    flag_last_measurement = True  # or False

    # List of dictionaries contains the id of each WS and its one of the Datastreams.
    # For WS where one of the Datastreams is missing the None value is filled
    WLDS = []
    PRDS = []

    flag_phenTime = True
    dates_WL_RT = [{'PhenDateTime': now_WL}]
    dates_PR_RT = [{'PhenDateTime': now_PR}]

    for i, StationID in enumerate(Weather_Stations_Names_Ids):

        # extract the location of the station- Filters
        SensThings_Loc = [SensorThingEntities[0], SensorThingEntities[1]]
        selVals = {'thing_sel': ['id', 'name'], 'loc_sel': ['location']}
        filt_args = {'thing_filt': ['id']}
        filt_vals = {'thing_filt': str(StationID['ID'])}

        # Query for the location
        resp_station_loc = extract_station_location(service_root_URI, SensThings_Loc, selVals, filt_args, filt_vals)

        SensThings = [SensorThingEntities[0], SensorThingEntities[3] , SensorThingEntities[5]]
        selVals = {'dstr_sel': ['id', 'name', 'phenomenonTime']}
        filt_args={'thing_filt': ['id'], 'dstr_filt': ['name']}

        # Check if the current station id is inside Threshold_PR same for WL bellow
        if any(d['ID'] == StationID['ID'] for d in Thresholds_PR):

            Result_PR = []
            PhenDateTime_PR = []
            IoT_ID_PR = []

            # Get The current alerts.
            x = next(item for item in Thresholds_PR if item["ID"] == StationID["ID"])['Alarm1']
            y = next(item for item in Thresholds_PR if item["ID"] == StationID["ID"])['Alarm2']
            z = next(item for item in Thresholds_PR if item["ID"] == StationID["ID"])['Alarm3']


            # 1-Yellow, 2-Orange, 3-Red
            Alarms_PR = [{"note": "Alarm Threshold 1", "xValue": "1", "yValue": x, "color": "#F8E71C"},
                         {"note": "Alarm Threshold 2", "xValue": "2", "yValue": y, "color": "#E68431"},
                         {"note": "Alarm Threshold 3", "xValue": "3", "yValue": z, "color": "#AA2050"}
                         ]


            PRDS_dict = {'ID': np.int64(StationID["ID"])}

            filt_vals_PR = {'thing_filt': str(StationID["ID"]), 'dstr_filt': ['Precipitation']}

            # Create Query
            # Use the query using the current Date Time
            # Get the datetime
            Current_Real_DateTime_PR = dates_PR_RT[0]['PhenDateTime']

            resp_station_datastream_PR = extract_station_datastream(service_root_URI, SensThings, selVals,filt_args, filt_vals_PR,
                                                                        Current_Real_DateTime_PR, flag_phenTime, Num_Interest_Obs)


            dset = resp_station_datastream_PR

            typeMeas_PR = dset['value'][0]['Datastreams'][0]['name'].startswith('Precipitation')


            if typeMeas_PR == True:

                # Update PRDS with Weather Station name
                PRDS_dict.update({'WS_name': dset['value'][0]['name']})
                PRDS_dict.update({'Coordinates': resp_station_loc['value'][0]['Locations'][0]['location']['coordinates']})

                # Check if the query is empty
                if len(dset['value'][0]['Datastreams']) == 0 or len(dset['value'][0]['Datastreams'][0]['Observations']) == 0:
                    PRDS_dict.update({'Precipitation': None})
                else:
                    PRDS_dict.update({'DS_ID': dset['value'][0]['Datastreams'][0]['@iot.id']})

                    # Check how many Observations we have and update the lists

                    for p in range(len(dset['value'][0]['Datastreams'][0]['Observations'])):
                        PhenDateTime_PR.append(
                            dset['value'][0]['Datastreams'][0]['Observations'][p][
                                'phenomenonTime'])
                        Result_PR.append(
                            dset['value'][0]['Datastreams'][0]['Observations'][p]['result'])
                        IoT_ID_PR.append(
                            dset['value'][0]['Datastreams'][0]['Observations'][p]['@iot.id'])

                    PRDS_dict.update({'PhenDateTime': PhenDateTime_PR})
                    PRDS_dict.update({'Precipitation': Result_PR})
                    PRDS_dict.update({'IoT_ID': IoT_ID_PR})


            # Compare threshold and update the dictionary
            # Check if we have results
            if len(Result_PR) > 0:

                Color_List_PR = []
                Note_List_PR = []
                Scale_List_PR = []
                Note_Scale_List_PR = []

                for tr in range(len(Result_PR)):
                    Thresh_Results_PR = compare_value_scale_thresholds_new(Result_PR[tr], StationID['ID'],
                                                                           filt_vals_PR['dstr_filt'], Thresholds_PR)

                    # Update each metric
                    Color_List_PR.append(Thresh_Results_PR[0])
                    Note_List_PR.append(Thresh_Results_PR[1])
                    Scale_List_PR.append(Thresh_Results_PR[2])
                    Note_Scale_List_PR.append(Thresh_Results_PR[3])

                PRDS_dict.update({'Color': Color_List_PR})
                PRDS_dict.update({'Note': Note_List_PR})
                PRDS_dict.update({'Scale': Scale_List_PR})
                PRDS_dict.update({'Note_Scale': Note_Scale_List_PR})

            else:
                PRDS_dict.update({'Color': None})
                PRDS_dict.update({'Note': None})
                PRDS_dict.update({'Scale': None})
                PRDS_dict.update({'Note_Scale': None})

            # Update the main List of dictionaries
            PRDS.append(PRDS_dict)

            print("\n ===> Testing reasons: \n")
            print(" PRDS = \n", PRDS)
            print("\n ==================== END PRDS========= \n")

            # CREATE TOPIC 104 PRECIPITATION FORWARDED TO THE DASHBOARD - LINE PLOT
            PRDS_last_meas = PRDS_dict
            PRDS_last_meas['PhenDateTime'] = [ PRDS_last_meas['PhenDateTime'][0] ]
            PRDS_last_meas['Precipitation'] = [ PRDS_last_meas['Precipitation'][0] ]
            PRDS_last_meas['IoT_ID'] = [ PRDS_last_meas['IoT_ID'][0] ]
            PRDS_last_meas['Color'] = [ PRDS_last_meas['Color'][0] ]
            PRDS_last_meas['Note'] = [ PRDS_last_meas['Note'][0] ]
            PRDS_last_meas['Scale'] = [ PRDS_last_meas['Scale'][0] ]
            PRDS_last_meas['Note_Scale'] = [ PRDS_last_meas['Note_Scale'][0] ]

            topic104FloodEmerg_Precipitation(directory, PRDS_last_meas, Alarms_PR, flag_mode, producer)
            count_top104_PR = count_top104_PR + 1

        else:
            print("\n **** Weather Station with ID: ", StationID['ID']," does not exist in Thresholds_PR list. \n")

        # Create Top006 if and only if the current Precipitation exceeds the 1st Alarm Threshold (Scale >= 2)
        for PRDS_item in PRDS:
            if PRDS_item['Scale'][-1] >= 2:

                topic104FloodEmerg_Precipitation_MAP_v2(directory, PRDS_item, producer)
                count_top104_PR_Map = count_top104_PR_Map + 1

                topic006_FloodEmerg_Precipitation_IR(directory, PRDS_item, producer)
                count_top006_PR = count_top006_PR + 1

        #------------------------------------------------------------------------------------------
        # Check for the Water Level incidents
        #
        # double IF, because we have 1 station ID for 2 Lists (WL/PR) -- 347
        #
        if any(d['ID'] == StationID['ID'] for d in Thresholds_WL):

            Result_WL = []
            PhenDateTime_WL = []
            IoT_ID_WL = []

            q = next(item for item in Thresholds_WL if item["ID"] == StationID['ID'])['Alarm1']
            r = next(item for item in Thresholds_WL if item["ID"] == StationID['ID'])['Alarm2']
            t = next(item for item in Thresholds_WL if item["ID"] == StationID['ID'])['Alarm3']


            Alarms_WL = [{"note": "Alarm Threshold 1", "xValue": "1", "yValue": q, "color": "#F8E71C"},
                         {"note": "Alarm Threshold 2", "xValue": "2", "yValue": r, "color": "#E68431"},
                         {"note": "Alarm Threshold 3", "xValue": "3", "yValue": t, "color": "#AA2050"}
                         ]

            WLDS_dict = {'ID': np.int64(StationID['ID'])}
            filt_vals_WL = {'thing_filt': str(StationID['ID']), 'dstr_filt': ['Water']}

            # Create Query

            # Use the query using the current Date Time
            # Get the datetime
            Current_Real_DateTime_WL = dates_WL_RT[0]['PhenDateTime']

            resp_station_datastream_WL = extract_station_datastream(service_root_URI, SensThings, selVals,filt_args, filt_vals_WL,
                                                                        Current_Real_DateTime_WL, flag_phenTime, Num_Interest_Obs)


            dset_WL = resp_station_datastream_WL
            # Extract the corresponding data (sensors measurements) from the xlsx file 'ObservedData_Sensors.xlsx'
            #
            # dset_WL = df_ObservedData[ (df_ObservedData['ID_WS'] == StationID["ID"]) & (df_ObservedData['DS_ID'] == int(session))]
            # dset_WL.reset_index(drop=True, inplace=True)

            typeMeas_WL = dset_WL['value'][0]['Datastreams'][0]['name'].startswith('Water')

            if typeMeas_WL == True:

                # Update WLDS Water Station name
                WLDS_dict.update({'WS_name': dset['value'][0]['name']})
                WLDS_dict.update({'Coordinates': resp_station_loc['value'][0]['Locations'][0]['location']['coordinates']})

                # Check if the query is empty
                if len(dset_WL['value'][0]['Datastreams']) == 0 or len(dset['value'][0]['Datastreams'][0]['Observations']) == 0:
                    WLDS_dict.update({'Water_Level': None})
                else:
                    WLDS_dict.update({'DS_ID': dset_WL['value'][0]['Datastreams'][0]['@iot.id']})

                    for p in range(len(dset_WL['value'][0]['Datastreams'][0]['Observations'])):
                        PhenDateTime_WL.append(
                            dset_WL['value'][0]['Datastreams'][0]['Observations'][p][
                                'phenomenonTime'])
                        Result_WL.append(
                            dset_WL['value'][0]['Datastreams'][0]['Observations'][p]['result'])
                        IoT_ID_WL.append(
                            dset_WL['value'][0]['Datastreams'][0]['Observations'][p]['@iot.id'])

                    WLDS_dict.update({'PhenDateTime': PhenDateTime_WL})
                    WLDS_dict.update({'Water_level': Result_WL})
                    WLDS_dict.update({'IoT_ID': IoT_ID_WL})

            # Compare threshold and update the dictionary
            # Check if we have results
            if len(Result_WL) > 0:

                Color_List_WL = []
                Note_List_WL = []
                Scale_List_WL = []
                Note_Scale_List_WL = []

                for rt in range(len(Result_WL)):
                    Thresh_Results_WL = compare_value_scale_thresholds_new(Result_WL[rt], StationID['ID'],
                                                                           filt_vals_WL['dstr_filt'], Thresholds_WL)

                    # Update each metric
                    Color_List_WL.append(Thresh_Results_WL[0])
                    Note_List_WL.append(Thresh_Results_WL[1])
                    Scale_List_WL.append(Thresh_Results_WL[2])
                    Note_Scale_List_WL.append(Thresh_Results_WL[3])

                WLDS_dict.update({'Color': Color_List_WL})
                WLDS_dict.update({'Note': Note_List_WL})
                WLDS_dict.update({'Scale': Scale_List_WL})
                WLDS_dict.update({'Note_Scale': Note_Scale_List_WL})

            else:
                WLDS_dict.update({'Color': None})
                WLDS_dict.update({'Note': None})
                WLDS_dict.update({'Scale': None})
                WLDS_dict.update({'Note_Scale': None})

            # Update the main List of dictionaries
            WLDS.append(WLDS_dict)

            print("\n ===> Testing reasons: \n")
            print(" WLDS = \n", WLDS)
            print("\n ==================== END WLDS========= \n")

            WLDS_last_meas = WLDS_dict
            WLDS_last_meas['PhenDateTime'] = [WLDS_last_meas['PhenDateTime'][0]]
            WLDS_last_meas['Water_level'] = [WLDS_last_meas['Water_level'][0]]
            WLDS_last_meas['IoT_ID'] = [WLDS_last_meas['IoT_ID'][0]]
            WLDS_last_meas['Color'] = [WLDS_last_meas['Color'][0]]
            WLDS_last_meas['Note'] = [WLDS_last_meas['Note'][0]]
            WLDS_last_meas['Scale'] = [WLDS_last_meas['Scale'][0]]
            WLDS_last_meas['Note_Scale'] = [WLDS_last_meas['Note_Scale'][0]]

            topic104FloodEmerg_WaterLevel(directory, WLDS_last_meas, Alarms_WL, flag_mode, producer)
            count_top104_WL = count_top104_WL + 1

        else:
            print("\n **** Weather Station with ID: ", StationID['ID'] ," does not exist in Thresholds_WL list. \n")

    # flname_WL = directory + "/" + 'responses_Sensors_Water_Level.txt'
    # with open(flname_WL, 'w') as outfile_WL:
    #     json.dump(WLDS, outfile_WL)
    #
    # flname_PR = directory + "/" + 'response_Sensors_Precitipation.txt'
    # with open(flname_PR, 'w') as outfile_PR:
    #     json.dump(PRDS, outfile_PR)


    # Create topics for the Map. For each WS where its WL exceed a pre-defined threshold then the corresponding Top104 is created and
    # forwarded to the PSAP.
    # Also, it creates and sends Top006 incident report to KB
    #
    for WLDS_item in WLDS:
        if WLDS_item['Scale'][-1] >= 2:
            topic104FloodEmerg_WaterLevel_MAP_v2(directory, WLDS_item, producer)
            count_top104_WL_Map = count_top104_WL_Map + 1

            topic006_FloodEmerg_WaterLevel_IR(directory, WLDS_item, producer)
            count_top006_WL = count_top006_WL + 1

    # End Timing Step 1
    end_step1 = time.time()
    time_duration_step.append( end_step1 - start_step1 )

    print('\n ------- Steps 1-3 are terminated ------- \n')

    #------------------------------------------------------------------------------------
    # STEP 4: Calculate the Overall Crisis Level index for all Weather Stations

    print('\n ------- Step 4: Calculate the Overall Crisis Level index for all Weather Stations ------- \n')

    # Start Timing Step 4
    start_step4 = time.time()

    # Number of observations that requested (Top parameter)
    Num_Interest_Obs = 1
    print("\n Num_Interest_Obs= ", Num_Interest_Obs)

    if len(WLDS) > 0:

        OCL_list = []
        for i in range(Num_Interest_Obs):

            meas_ColNote_WL = []
            for WLDS_item in WLDS:

                meas_ColNote_WL_dict = {'ID': WLDS_item['ID'], 'col': WLDS_item['Color'][i], 'note': WLDS_item['Note'][i],
                                    'scale': WLDS_item['Scale'][i], 'note_scale': WLDS_item['Note_Scale'][i]}
                meas_ColNote_WL.append( meas_ColNote_WL_dict )

            ocl_ws_val = Overall_Crisis_Level_WeatherStations(meas_ColNote_WL)
            # Gia na parei thn teletuaia hmerominia
            ocl_ws_val.update({'PhenDateTime': WLDS[0]['PhenDateTime']})

            OCL_list.append(ocl_ws_val)
            print("\n ----------------------- ")
            print("ocl_ws_val =", ocl_ws_val )
            print("--------------------------\n")

        # Create topic

        print( "len(OCL_list) =", len(OCL_list) )

        topic104FloodEmerg_OverallCrisisLevel(flag_mode, directory, OCL_list[0], producer )
        count_top104_OCL = count_top104_OCL + 1

    else:
        print("\n No Flood events took place in current date/time !!! \n")

    # End Timing Step 4
    end_step4 = time.time()
    time_duration_step.append( end_step4 - start_step4 )

    #---------------------------------------------------------------------------
    total_time = np.array(time_duration_step).sum()

    print("\n ****** EXECUTION TIME: **** ")
    Time_steps_1_3 = round(time_duration_step[0], 3)
    Time_steps_1_3_min = round(time_duration_step[0]/ 60.0, 3)
    Time_step_4 = round(time_duration_step[1], 3)
    Time_step_4_min =  round(time_duration_step[1]/60.0, 3)

    Total_Time_min = round(total_time/60.0, 3)

    # LIST OF TIMES
    Timers_lists = [['Steps 1-3', Time_steps_1_3, Time_steps_1_3_min],
                    ['Step 4', Time_step_4, Time_step_4_min],
                    ['Total Time', total_time, Total_Time_min]
                    ]

    Timers = pd.DataFrame(Timers_lists, columns=['Timer', 'Seconds', 'Minutes'])


    Counters_list = [ ['Topics_104 for dashboard', count_top104_WL, count_top104_PR ],
                      ['Topics 104 for map', count_top104_WL_Map, count_top104_PR_Map],
                      ['Topics 006', count_top006_WL, count_top006_PR],
                      ['Topic 104 Overall Crisis level', count_top104_OCL, None]]

    Counters = pd.DataFrame(Counters_list, columns = ['Description', 'Water Level', 'Precipitation'])

    xlsEv = pd.ExcelWriter(directory + "/" + "Evaluation_RealData_Results.xlsx")
    Timers.to_excel(xlsEv, 'Evaluation_Time', index=False)
    Counters.to_excel(xlsEv,'Evaluation_Counter',index=False)
    xlsEv.save()

    print(" Time for Step 1 to 3: ", Time_steps_1_3, " seconds")
    print(" Time for Step 4. Calculate OFLCL & create Topics 104: ", Time_step_4, " seconds")
    print(" Total Execution Time: ", total_time, "seconds -> ", Total_Time_min, " minutes")
    print(" Topics_104 WL for dashboard: ", count_top104_WL)
    print(" Topics_104 PR for dashboard: ", count_top104_PR)
    print(" Topics_104 WL for map: ", count_top104_WL_Map)
    print(" Topics_104 PR for map: ", count_top104_PR_Map)
    print(" Topics_006 WL: ", count_top006_WL)
    print(" Topics_006 PR: ", count_top006_PR)

    print("\n This task has been completed!!! CRCL flood real-time emergency phase waits for another trigger message!!! \n")

    print("\n ****** EXECUTION TIME: **** ")
    print(" Time for Step 1 to 3: ", round(time_duration_step[0], 3), " seconds")
    print(" Time for Step 4. Calculate OFLCL & create Topics 104: ", round(time_duration_step[1], 3), " seconds")
    print(" Total Execution Time: ", round(total_time/60.0, 3), " minutes")


# end function


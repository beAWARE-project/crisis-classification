# Created Date: 03/05/2019
# Modified Date:
# Last Modified:
#
# -------------------------------- 2nd PROTOTYPE VERSION BASED ON VICENZA PILOT --------------------------------------------------
#
# Implements the algorithm of Crisis Classification for the flood pilot based on the Incident Reports of
# citizens' and first responders' mobile App
#
# Inputs are:
#   - coordinate of the incident
#   - DateTime of the incident
#   - value of the Category field
#   - value of the Hazard field (estimated water level by the citizen or first responder)
#   - values of the Exposure field (2 values at most)
#   - URI = 'https://www.iosb.fraunhofer.de/ilt/mobileApp#test_generic_flood_report'
#   - data from GIS files

import os, time
import pandas as pd
from datetime import datetime, timedelta
from CRCL.FloodCRisisCLassification.Auxiliary_Functions_Risk_v02 import *
from CRCL.FloodCRisisCLassification.Create_Queries import *
from CRCL.FloodCRisisCLassification.topic104_Flood_Emerg import *
from CRCL.FloodCRisisCLassification.topic007_Flood_Emerg_Update_Incident_Risk import *

from CRCL.FloodCRisisCLassification.Auxiliary_Functions_DB import *

from bus.bus_producer import BusProducer
from collections import OrderedDict


def CrisisClassificationFlood_Emerg_IncRep_v2(cur_date, URI, Incident_loc, URI_time, Incident_ID, Incident_Description):

    # # Start Timing Step 1 - Extract info from Incident Report
    start_step1 = time.time()

    # Store the time steps
    time_duration_step = []

    # Query_Dict contains Quantification, ReportType, Category
    Query_Dict = URI_query(URI)
    print()
    print('URI from the TOP105DEV: ', URI)
    print()
    print('Time received: ', URI_time)
    print()
    print('Location from the incident report: ', Incident_loc)
    print()
    print('Incident Description: ', Incident_Description)

    if len(Query_Dict['Quantification']) >= 1:
        Hazard_field = Query_Dict['Quantification'][0]
    else:
        Hazard_field = None

    exposure_field_rep = Query_Dict['ReportType']
    # Category_field = Query_Dict['Category'][0]

    # Setting the write path
    if len(Query_Dict['Category']) > 0:
        Category_field = Categorise_category(Query_Dict['Category'][0])
    else:
        Category_field = None

    # Break the execution of this function
    if Category_field == 'Unknown':
        print("\n Program is terminated without do anything -- Unknown category !!!\n")
        return

    if len(exposure_field_rep) == 1 and exposure_field_rep[0] != 'NoneOfTheAbove':
        Exposure_field = ['people_involved', exposure_field_rep[0]]
    else:
        Exposure_field = ['people_involved']  # user chooses None

    print("\n Indent ID = ", Incident_ID )
    print("\n Hazard_field = ", Hazard_field)
    print("\n Exposure_field = ", Exposure_field)
    print("\n Category = ", Query_Dict['Category'], " and Category_field = ", Category_field)

    ver = 'Emerg_IncRep_2nd_PROTOTYPE'

    # Create a directory to store the output files and TOPICS
    # Create a path
    current_dirs_parent = os.getcwd()
    root_path_dir = current_dirs_parent + "/" + "CRCL/FloodCRisisCLassification" + "/"

    now_dt = cur_date.isoformat()
    directory = root_path_dir + ver + "_" + "TOPICS" + "_" + "Inc" + "_" + Incident_ID + "_" + str(now_dt)
    os.makedirs(directory, exist_ok=True)

    # End Timing Step 1
    end_step1 = time.time()
    time_duration_step.append( {'Description':'Step 1 - Extract info from Incident Report',
                                'Time Step in sec.': end_step1 - start_step1 } )

    print('\n ------- Steps 1 is terminated ------- \n')

    #------------------------------------------------------------
    producer = BusProducer()

    # Decorate terminal
    print('\033[95m' + "\n***********************")
    print("*** CRCL SERVICE v1.0 ***")
    print("***********************\n" + '\033[0m')

    #---------------------------------------------------------------------------------------------------------
    # Step 2: Calculate the Risk
    # 2A: Extract data from GIS for Hazard field, if it is needed
    # 2B: Estimate Hazard value
    # 2C: Calculate Exposure
    # 2D: Use GIS for exposure estimation
    # 2E: Calculate vulnerability based on Hazard and Exposure fields
    # 2F: Calculate the Hydraulic Risk and Severity

    point_of_interest = {"long": float(Incident_loc['longitude']), "lat": float(Incident_loc['latitude'])}

    # Category field == 1 or Category field == void then calculate Hydraulic Risk
    if Category_field == "1" or Category_field is None:

        print('Category field = ', Category_field)
        print('Hazard field = ', Hazard_field)

        # Determine the WL by GIS or Social media
        if Hazard_field is None:

            # Start Timing Step 2A Extract data from GIS for Hazard field, if it is needed
            start_step2A = time.time()

            print("\n Need to extract from GIS shapefiles WH_HMP.shp OR from Social media \n")

            # Water level from Social media -- TBD

            # the Water Level will be obtained from the corresponding shapefile of the GIS by creating a query to
            # Geoserver
            #
            geoserver_name = '100YearHigh'

            width_step = 0.0001
            geo100_resp_hazard = extract_WL_gis(geoserver_name, point_of_interest, width_step )

            len_geo100_resp_hazard = len(geo100_resp_hazard['features'])
            print(" Number of Features = ", len_geo100_resp_hazard )

            # write Geo-query reply in JSON to output file
            flname = directory + "/" + 'geoServer100_response.txt'
            with open(flname, 'w') as outfile:
                json.dump(OrderedDict(geo100_resp_hazard), outfile)

            # Find the appropriate polygon (or bbox) that point is belong to.
            # If more than one polygons are found then each polygon with its feature id
            # height and its category id are stored as a dictionary in an array.
            #
            count_true_100 = 0
            count_false_100 = 0
            count_true_300 = 0
            count_false_300 = 0

            Features_of_Interest = []
            if len_geo100_resp_hazard > 0:

                for i in range(len(geo100_resp_hazard['features'])):

                    cand_bbox_100 = geo100_resp_hazard['features'][i]['properties']['bbox']
                    cand_poly_100 = geo100_resp_hazard['features'][i]['geometry']['coordinates'][0][0]

                    # print("\n cand_poly = ", cand_poly)
                    # print("\n", Incident_loc['long'], " -- ", Incident_loc['lat'], "\n")

                    # flagIsIns = isInside(Incident_loc, cand_bbox)
                    flagIsIns =  point_in_polygon(point_of_interest, cand_poly_100)

                    if flagIsIns == True:

                        print('Point IS inside in the feature with: ', geo100_resp_hazard['features'][i]['id'])
                        print('Height = ', geo100_resp_hazard['features'][i]['properties']['HEIGHT'])
                        print(cand_bbox_100)

                        # Correspond HEIGHT with WL from Mobile App and determine its category
                        HEIGHT_shp = geo100_resp_hazard['features'][i]['properties']['HEIGHT']

                        if HEIGHT_shp == '0-50 cm':
                            WaterLevel = '0.25To0.5'
                            WL_Categ_ID = '1'
                        elif HEIGHT_shp == '50-100 cm':
                            WaterLevel = '0.75To1'
                            WL_Categ_ID = '3'
                        elif HEIGHT_shp == '100-200 cm' or HEIGHT_shp == '>200 cm':
                            WaterLevel = 'W.L.more1m'  #'MoreThan1'
                            WL_Categ_ID = '4'

                        item ={ 'Feature_Id': geo100_resp_hazard['features'][i]['id'],
                                'Height' : HEIGHT_shp,
                                'WaterLevel': WaterLevel,
                                'WL_Categ_ID': WL_Categ_ID }

                        Features_of_Interest.append(item)
                        count_true_100 += 1

                        # Estimate the Hazard_field for the Features_of_Interest
                        pm = max(enumerate(Features_of_Interest), key=lambda item: item[1]['WL_Categ_ID'])
                        Hazard_field = Features_of_Interest[pm[0]]['WaterLevel']

                    else:
                        print('Point is not inside in the feature with: ', geo100_resp_hazard['features'][i]['id'])
                        print(cand_bbox_100)
                        count_false_100 += 1

            elif len_geo100_resp_hazard == 0:

                # Check geoserver 300 years shapefile
                geoserver_name = 'awaa_water_level_TR300' # '300YearHigh' #

                width_step = 0.0001
                geo300_resp_hazard = extract_WL_gis(geoserver_name, point_of_interest, width_step)

                len_geo300_resp_hazard = len(geo300_resp_hazard['features'])
                print(" Number of Features = ", len_geo300_resp_hazard)

                # write Geo-query reply in JSON to output file
                flname = directory + "/" + 'geoServer300_response.txt'
                with open(flname, 'w') as outfile:
                    json.dump(OrderedDict(geo300_resp_hazard), outfile)

                # Find the appropriate polygon (or bbox) that point is belong to.
                # If more than one polygons are found then each polygon with its feature id
                # height and its category id are stored as a dictionary in an array.
                #

                for i in range(len(geo300_resp_hazard['features'])):

                    cand_bbox_300 = geo300_resp_hazard['features'][i]['properties']['bbox']
                    cand_poly_300 = geo300_resp_hazard['features'][i]['geometry']['coordinates'][0][0]


                    flagIsIns = point_in_polygon(point_of_interest, cand_poly_300)

                    if flagIsIns == True:

                        print('Point IS inside in the feature with: ', geo300_resp_hazard['features'][i]['id'])
                        print('Height = ', geo300_resp_hazard['features'][i]['properties']['HEIGHT'])
                        print(cand_bbox_300)

                        # Correspond HEIGHT with WL from Mobile App and determine its category
                        HEIGHT_shp = geo300_resp_hazard['features'][i]['properties']['HEIGHT']

                        if HEIGHT_shp == '0-50 cm':
                            WaterLevel = '0.25To0.5'
                            WL_Categ_ID = '1'
                        elif HEIGHT_shp == '50-100 cm':
                            WaterLevel = '0.75To1'
                            WL_Categ_ID = '3'
                        elif HEIGHT_shp == '100-200 cm' or HEIGHT_shp == '>200 cm':
                            WaterLevel = 'W.L.more1m'  # 'MoreThan1'
                            WL_Categ_ID = '4'

                        item = {'Feature_Id': geo300_resp_hazard['features'][i]['id'],
                                'Height': HEIGHT_shp,
                                'WaterLevel': WaterLevel,
                                'WL_Categ_ID': WL_Categ_ID}

                        Features_of_Interest.append(item)
                        count_true_300 += 1

                        # Estimate the Hazard_field for the Features_of_Interest
                        pm = max(enumerate(Features_of_Interest), key=lambda item: item[1]['WL_Categ_ID'])
                        Hazard_field = Features_of_Interest[pm[0]]['WaterLevel']

                    else:
                        print('Point is not inside in the feature with: ', geo300_resp_hazard['features'][i]['id'])
                        print(cand_bbox_300)
                        count_false_300 += 1


            # if there is no items from 100 neither 300, then there is no flood. Or the area is fault
            if len(Features_of_Interest) == 0:
                item = {'WaterLevel': 0}
                Features_of_Interest.append(item)

                Hazard_field = Features_of_Interest[0]['WaterLevel']

            print("\n ---->>>> Testing Reasons: \n")
            print(Features_of_Interest)
            print('Count true for 100: ', count_true_100)
            print('Count true for 300: ', count_true_300)
            print('Count false for 100: ', count_false_100)
            print('Count false for 300: ', count_false_300)

            print("Hazard_field = ", Hazard_field)

            # End Timing Step 2A
            end_step2A = time.time()
            time_duration_step.append( {'Description':'Step 2A - Extract data from GIS for Hazard field, if it is needed.',
                                'Time Step in sec.': end_step2A - start_step2A } )

            # Estimate hazard value
            start_step2B = time.time()

            hazard_value = calculate_hazard(Hazard_field)  # output as dictionary {'Value', 'Description'}
            print("hazard_value = ", hazard_value)

            # End Timing Step 2B
            end_step2B = time.time()
            time_duration_step.append( {'Description':'Step 2B - Estimate Hazard value',
                                        'Time Step in sec.': end_step2B - start_step2B } )

        #---------------------------------------------------------
        # Step 2.1: Calculate the Hazard. Hazard_field is not void
        elif Hazard_field != None:

            time_duration_step.append( {'Description': 'Step 2A - No need to use GIS', 'Time Step in sec.': 'None' } )

            # Estimate hazard value time
            start_step2B = time.time()

            hazard_value = calculate_hazard(Hazard_field)  # output as dictionary {'Value', 'Description'}
            print("hazard_value = ", hazard_value)

            # End Timing Step 2B
            end_step2B = time.time()
            time_duration_step.append({'Description':'Step 2B - Estimate Hazard value',
                                        'Time Step in sec.': end_step2B - start_step2B } )

        #---------------------------------------------------------------
        # Step 2C: Calculate Exposure

        if len(Exposure_field) == 2:

            # Estimate Exposure time Step 2C
            start_step2C = time.time()

            # the output is the exposure for the people Ep, economic activities Ee and Cultural asset Ea
            Ep, Ee, Ea = calculate_exposure(Exposure_field)

            # End Timing Step 2C
            end_step2C = time.time()
            time_duration_step.append({'Description':'Step 2C - Calculate Exposure',  'Time Step in sec.': end_step2C - start_step2C })

            time_duration_step.append( {'Description': 'Step 2D - No need to use GIS', 'Time Step in sec.': 'None' } )

        elif len(Exposure_field) == 1 and Exposure_field[0] != 'people_involved':

            # Estimate Exposure time Step 2C
            start_step2C = time.time()

            # the output is the exposure for the people Ep, economic activities Ee and Cultural asset Ea
            Ep, Ee, Ea = calculate_exposure(Exposure_field)

            # End Timing Step 2C
            end_step2C = time.time()
            time_duration_step.append({'Description':'Step 2C - Calculate Exposure',  'Time Step in sec.': end_step2C - start_step2C })

            time_duration_step.append( {'Description': 'Step 2D - No need to use GIS', 'Time Step in sec.': 'None' } )

        elif (len(Exposure_field) == 1 and Exposure_field[0] == 'people_involved') or (len(Exposure_field) == 0):
            # Exposure field contains only one category that is 'People involved'
            # so the estimation of "Element at Risk" is carried out by using GIS or social media

            ##################################### ELement #############################
            print("\n Need to extract from GIS shapefiles LandUse.shp attribute CLASSE OR from Social media - TBD \n")

            # Estimate time for using GIS in order to estimate the Exposure
            start_step2D = time.time()

            # Extract the class of element at risk using the GIS file
            geoserver_shapefile_name = 'LandUse'

            # Return the element at risk near the point_of_interest. If no element is found the list contains one item indicating as
            # "Unknown". Output is a list of dictionary: { Feature_Id', 'Class', 'Categ_ElemOfRisk' }
            #
            Elements_at_risk = find_element_at_risk_GIS(directory, geoserver_shapefile_name, point_of_interest)

            # Append the element at risk in the existing Exposure_field list
            #
            # TBD: if there more that one elements at risk ????
            #
            Exposure_field.append(Elements_at_risk[0]['Categ_ElemOfRisk'])
            print("\n ===>>>> New Exposure_field list = ", Exposure_field)

             # End Timing Step 2D
            end_step2D = time.time()

            ################################################# Exposure ###########################
            #
            # Estimate Exposure time Step 2C
            start_step2C = time.time()

            if Elements_at_risk[0]['Categ_ElemOfRisk'] != 'Unknown':

                 # the output is the exposure for the people Ep, economic activities Ee and Cultural asset Ea
                 elem_at_risk = [ Elements_at_risk[0]['Categ_ElemOfRisk'] ]
                 Ep, Ee, Ea = calculate_exposure( elem_at_risk )

            else:
                # Calculate the Ep, Ee, Ea from GIS 'awaa_efinale_hmp' (EFINALE_HMP.shp)
                geoserver_shapefile_name = 'awaa_efinale_hmp'
                Exposure_item = calculate_exposure_from_GIS(directory, geoserver_shapefile_name, point_of_interest)

                Ep = Exposure_item[0]['Ep']
                Ee = Exposure_item[0]['Ee']
                Ea = Exposure_item[0]['Ea']

            # print("\n ------------------ Exposure: ")
            # print("Ep = ", Ep , " | Ee = ", Ee, " | Ea = ", Ea, "\n")

             # End Timing Step 2C
            end_step2C = time.time()
            time_duration_step.append({'Description':'Step 2C - Calculate Exposure',
                                       'Time Step in sec.': end_step2C - start_step2C })

            # Append time for step 2D
            time_duration_step.append({'Description': 'Step 2D - Use GIS for exposure estimation',
                                       'Time Step in sec.': end_step2D - start_step2D})

        #-----------------------------------------------------------------------------------------------
        # Steps 2E: Calculate vulnerability based on Hazard and Exposure fields
        # The output is the Vulnerabilities for people Vp, economic activities Ve and Cultural asset Va
        #

        # Estimate time for Step 2E: Calculate vulnerability based on Hazard and Exposure fields
        start_step2E = time.time()

        Vp, Ve, Va = calculate_vulnerability(Hazard_field, Exposure_field)

        print("Hazard =", round(hazard_value['Value'], 2))
        print("Vp =", Vp)
        print("Ve =", Ve)
        print("Va =", Va)

        print("Ep =", Ep)
        print("Ee =", Ee)
        print("Ea =", Ea)
        print('Exposure Field:', Exposure_field)

        # End Timing Step 2C
        end_step2E = time.time()
        time_duration_step.append({'Description':'Step 2E - Calculate vulnerability',
                                    'Time Step in sec.': end_step2E - start_step2E })

        #------------------------------------------------------------------------------------------------
        # Step 2F: Calculate the Hydraulic Risk and Severity
        # output is a dictionary with keys { 'Hydraulic Risk' , 'Severity' }

        # Estimate time for Step 2F for Hydraulic Risk and Severity calculations
        start_step2F = time.time()

        Hydraulic_Risk_Severity = calculate_Hydraulic_Risk( hazard_value['Value'], Vp, Ve, Va, Ep, Ee, Ea )

        print("Hydraulic_Risk = ", Hydraulic_Risk_Severity['Hydraulic_Risk'])
        print("Severity = ", Hydraulic_Risk_Severity['Severity'])

        # End Timing Step 2
        end_step2F = time.time()
        time_duration_step.append({'Description':'Step 2F - Calculate the Hydraulic Risk and Severity',
                                    'Time Step in sec.': end_step2F - start_step2F })


        # Total time for steps 1 and 2
        Time_step_1 = time_duration_step[0]['Time Step in sec.']
        Time_step_2 = 0
        for item in time_duration_step:
            if item['Time Step in sec.'] != 'None' and item['Description'].startswith('Step 2') :
                Time_step_2 =  Time_step_2 + item['Time Step in sec.']

        total_time = Time_step_1 + Time_step_2

        # ------------------------------------------------------------------------------------------------------------------
        # Step 3: Create a dictionary to append it into the dataframe of results into Database
        #
        start_step3 = time.time()

        IncRep_dict = {'Incident_ID': Incident_ID, 'Incident_DateTime': URI_time, 'Hazard_field': Hazard_field,
                       'Exposure_field': Exposure_field, 'Category_field': Category_field, 'Inc_Long': point_of_interest['long'],
                       'Inc_Lat': point_of_interest['lat'], 'Hazard_value': hazard_value, 'Exposure_people':Ep, 'Exposure_economic_act': Ee,
                       'Exposure_cultural_asset': Ea, 'Vulnerability_people': Vp, "Vulnerability_economic_act": Ve,
                       "Vulnerability_cultural_asset": Va, 'Hydraulic_Risk': Hydraulic_Risk_Severity['Hydraulic_Risk'],
                       'Severity': Hydraulic_Risk_Severity['Severity'], 'Time_Step_1': Time_step_1, 'Time_Step_2': Time_step_2,
                       'Total_Time': total_time
                       }

        # Insert dictionary to DataBase

        insert_IncReport_to_db(IncRep_dict)

        # End Timing Step 3
        end_step3 = time.time()
        time_duration_step.append({'Description':'Step 3 - Update DB table Incident_Reports with new incident',
                                    'Time Step in sec.': end_step3 - start_step3 })


        # Step 4: Send topic 007 for update risk --> KB
        #
        start_step4 = time.time()

        topic007Flood_Update_Incident_Risk(directory, IncRep_dict, Incident_Description, producer)

        end_step4 = time.time()
        time_duration_step.append({'Description':'Step 4 - Create and send topic 007 for update risk to KB',
                                    'Time Step in sec.': end_step4 - start_step4 })

        # -----------------------------------------------------------------------------------------------------------------------------------
        #
        # Step 5: Calculate Total Risk Assessment

        start_step5 = time.time()

        # Read from Database the records of the table 'Flood_CRCL_DB'
        #
        db_name = 'Flood_CRCL_DB'
        table_name = 'Incident_Reports'

        df_Inc_Results = extract_IncReports_from_db(db_name, table_name)

        no_IncReports = len(df_Inc_Results)
        print("\n Number of Incident Reports = ", no_IncReports)

        # Voting method
        Total_Risk_Assessment = calculate_risk_assessment( df_Inc_Results  )

        # Generalised mean
        # Estimate the total risk and severity by generalised mean method
        categories = [{'Class': 'Minor', 'Scale': 1}, {'Class': 'Moderate', 'Scale': 2},
                      {'Class': 'Severe', 'Scale': 3}, {'Class': 'Extreme', 'Scale': 4},
                      {'Class': 'Unknown', 'Scale': 0}]
        power = len(categories) - 1  # exclude unknown class

        Total_Risk_Assessment_gm = calculate_risk_assessment_gm( df_Inc_Results, categories, power )

        print("\n Total_Risk_Assessment with voting method = ", Total_Risk_Assessment)
        print("\n ******************************* ")
        print("\n Total_Risk_Assessment with Generalised Mean method = ", Total_Risk_Assessment_gm )

        end_step5 = time.time()
        time_duration_step.append({'Description':'Step 5 - Calculate the Total Risk Assessment',
                                    'Time Step in sec.': end_step5 - start_step5 })

        # -----------------------------------------------------------------------------------------------------------------------------------
        # Step 6: Store the results to the table into the database
        #
        start_step6 = time.time()

        risk_assess_tuple = { 'Incident_ID': Incident_ID, 'Incident_DateTime': URI_time,
                              'Inc_Long': point_of_interest['long'], 'Inc_Lat': point_of_interest['lat'],
                              'Total_Risk_Assessment_Vot': Total_Risk_Assessment['Total_Risk_Assessment'],
                              'Total_Severity_Vot': Total_Risk_Assessment['Total_Severity'],
                              'Total_Category_Vot': Total_Risk_Assessment['Total_Category'],
                              'Total_Risk_Assessment_GM': Total_Risk_Assessment_gm['Total_Risk_Assessment'],
                              'Total_Severity_GM': Total_Risk_Assessment_gm['Total_Severity'],
                              'Total_Category_GM': Total_Risk_Assessment_gm['Total_Category']}

        insert_Risk_Assessment_to_db( risk_assess_tuple )

        end_step6 = time.time()
        time_duration_step.append({'Description':'Step 6 - Store the results of Total Risk Assessment into DB Table',
                                    'Time Step in sec.': end_step6 - start_step6 })

        # -------------------------------------------------------------------------------------------------------------------------------------
        # Step 7: Create Topics for the Incident Reports --> Dashboard charts for Risk Assessment (gauge) and Traffic light for severity distribution
        #
        start_step7 = time.time()

        # Topic for the Risk Assessment plot in the Dashboard
        topic104FloodEmerg_Risk(directory, Total_Risk_Assessment, point_of_interest, URI_time, producer)

        # Topic for the Severity plot in the Dashboard
        topic104Flood_Traffic_IRs_Dashboard(directory, Total_Risk_Assessment['Class_distribution'], producer)

        end_step7 = time.time()
        time_duration_step.append({'Description':'Step 7 - Create Topics for the Dashboard charts ',
                                    'Time Step in sec.': end_step7 - start_step7 })

    # elif Category_field != "1" and Category_field is not None:
    #     print("Category field = ", Category_field)
        # Implement section 4.2 from doc file


    print("\n ****** EXECUTION TIME: **** ")
    for time_it in time_duration_step:
        print( time_it['Description'], ": ", time_it['Time Step in sec.'], " seconds. \n" )


    Timers = pd.DataFrame(time_duration_step, columns=['Description', 'Time Step in sec.'])
    xlsEv = pd.ExcelWriter(directory + "/" + "Time_Evaluation_Emerg_IncReports_Results.xlsx")
    Timers.to_excel(xlsEv, 'Evaluation_Time', index=False)
    xlsEv.save()


    print("\n Program is terminated successfully!!!\n")



#================================================================================================================================
#
#       AUXILIARY FUNCTIONS FOR ALGORITHM 3 (INCIDENT REPORTS)
#
#
import pandas as pd
from math import pow, ceil
from collections import OrderedDict
import requests
import urllib.request
import json
import requests
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from CRCL.FloodCRisisCLassification.Create_Queries import *



def URI_query(URI):
    ##########################################
    # Split and create the query
    ##########################################
    dict = {}
    Quantification = []
    ReportType_c = []
    ReportType = []
    Category = []

    # URI individuals name
    individual_name = URI.split('#')[1]
    individuals_objectProperty = URI.split('#')[0]

    query_start = 'https://beaware-1.eu-de.containers.appdomain.cloud/servlet/is/Entry.4554.GetIndividuals/?'
    query_uri = 'uri=https%3A%2F%2Fwww.iosb.fraunhofer.de%2Filt%2FmobileApp%23' + individual_name
    query_end = '&addDataTypeProperties=true'
    query = query_start + query_uri + query_end
    print(query)

    ##########################################
    # Login with John account and run the query
    ##########################################
    s = requests.Session()
    # all cookies received will be stored in the session object
    post_fields = {'aspect': 'doLogin', 'user': 'John', 'key': '2ge1WepD'}
    authUrl = 'https://beaware-1.eu-de.containers.appdomain.cloud/servlet/is/rest/grantee/login/'

    s.post(authUrl, data=post_fields)

    r = s.get(query)
    response = r.json()

    ##########################################
    # Browse json
    ##########################################
    # Path for the Object Properties
    ObjectProperties_Response = response['individuals'][URI]['objectProperties']

    # Get the Quantification,ReportType,Category response from the query
    if individuals_objectProperty + '#hasQuantification' in response['individuals'][URI]['objectProperties']:
        Quant_Response = ObjectProperties_Response[individuals_objectProperty + '#hasQuantification']
        Quantification.append(Quant_Response[0].split('#')[1])

    if individuals_objectProperty + '#hasReportType' in response['individuals'][URI]['objectProperties']:
        ReportType_Response = ObjectProperties_Response[individuals_objectProperty + '#hasReportType']
        # Get the Report Type, possible more than 1
        for r in range(len(ReportType_Response)):
            ReportType_c.append(ReportType_Response[r].split('#')[1])

    if individuals_objectProperty + '#hasCategory' in response['individuals'][URI]['objectProperties']:
        Category_Response = ObjectProperties_Response[individuals_objectProperty + '#hasCategory']
        Category.append(Category_Response[0].split('#')[1])

        # Checking if the people is first choice, if not make it first
    if len(ReportType_c) == 2:
        if ReportType_c[0] != 'people_involved':
            ReportType.append(ReportType_c[1])
            ReportType.append(ReportType_c[0])
        elif ReportType_c[0] == 'people_involved':
            ReportType.append(ReportType_c[0])
            ReportType.append(ReportType_c[1])
    elif len(ReportType_c) == 1:
        ReportType.append(ReportType_c[0])

    # Get the list inside a dictionary
    dict.update({'Quantification': Quantification})
    dict.update({'ReportType': ReportType})
    dict.update({'Category': Category})

    return dict

#------------------------------------------------------------
# Correspond categories with IDs // Category Field options
#
def Categorise_category(Category):
    # Category field options
    if Category == 'generic_flood_report':
        Categorise = '1'
    elif Category == 'viability_report':
        Categorise = '2'
    elif Category == 'people_in_danger':
        Categorise = '3'
    elif Category == 'animals_in_danger':
        Categorise = '4'
    elif Category == 'river_overtopping':
        Categorise = '5'
    elif Category == 'river_breach':
        Categorise = '6'
    elif Category == 'bridge_obstruction':
        Categorise = '7'
    elif Category == 'urban_drainage_flood_report':
        Categorise = '8'
    else:
        Categorise = 'Unknown'

    return Categorise


#------------------------------------------------------
def isInside(point_coord, bbox):

    flag_isInside = False
    if bbox[0] < point_coord['long'] < bbox[2] and bbox[1] < point_coord['lat'] < bbox[3]:
        flag_isInside = True
    else:
        flag_isInside = False

    return(flag_isInside)

#--------------------------------------------------------
# Determine if a point is inside a given polygon or not
# Polygon is a list of (x,y) pairs. This function
# returns True or False.

def point_in_polygon(point_coords, poly):

    n = len(poly)
    inside = False

    x = point_coords['long']
    y = point_coords['lat']

    p1x,p1y = poly[0]
    for i in range(n+1):
        p2x,p2y = poly[i % n]
        if y > min(p1y,p2y):
            if y <= max(p1y,p2y):
                if x <= max(p1x,p2x):
                    if p1y != p2y:
                        xints = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
                    if p1x == p2x or x <= xints:
                        inside = not inside
        p1x,p1y = p2x,p2y

    return(inside)

#-----------------------------------------------------
def calculate_hazard(hf_val):

    if hf_val == 'less0.25':
        Hazard = {'Value': 0.4, 'Description': 'Low hazard'}
    elif hf_val == '0.25To0.5':
        Hazard = {'Value': 0.4, 'Description': 'Low hazard'}
    elif hf_val == '0.5To0.75':
        Hazard = {'Value': 0.8, 'Description': 'Medium hazard'}
    elif hf_val == '0.75To1':
        Hazard = {'Value': 0.8, 'Description': 'Medium hazard'}
    elif hf_val == 'W.L.more1m':
        Hazard = {'Value': 1.0, 'Description': 'High hazard'}
    else:
        Hazard = {'Value': 0, 'Description': 'No Hazard'}

    return(Hazard)

#--------------------------------------
def calculate_exposure(Exp_fld):

    if len(Exp_fld) == 2:
        element_at_risk = Exp_fld[1]
    elif len(Exp_fld) == 1:
        element_at_risk = Exp_fld[0]

    if element_at_risk == 'buildings':
        Ep = 1
        Ee = 1
        Ea = 0.9
    elif element_at_risk == 'infrastructure':
        Ep = 0.5
        Ee = 1
        Ea = 0.2
    elif element_at_risk == 'rural_area':
        Ep = 0.3
        Ee = 0.6
        Ea = 0.7
    elif element_at_risk == 'camping_ground':
        Ep = 1
        Ee = 1
        Ea = 0.1
    elif element_at_risk == 'natural_semi_natural_area':
        Ep = 0.1
        Ee = 0.1
        Ea = 1

    # need to be confirmed
    # if len(Exp_fld) == 1:
    #     Ep = 0

    return(Ep, Ee, Ea)

#--------------------------------------------------------------------------------------------------
# Calculates the Vulnerability based on the value of WL and
# element_at_risk (exposure field) coming from the APP
#
# Output: Vulnerability which consists of Vp (vulnerability of people),
#            Ve (vulnerability of economic activities)
#        and Va (vulnerability of environments and cultural assets)
#
def calculate_Ve(wl, element_at_risk):

    if wl == 0:
        wl = 'less0.25'

    if element_at_risk == 'buildings':
        if wl == 'less0.25' or wl == '0.25To0.5':
            Ve = 0.25
        elif wl == '0.5To0.75' or wl == '0.75To1':
            Ve = 0.75
        elif wl == 'W.L.more1m':
            Ve = 1
    elif element_at_risk == 'camping_ground':
        if wl == 'less0.25':
            Ve = 0.25
        elif wl == '0.25To0.5' or wl == '0.5To0.75':
            Ve = 0.75
        elif wl == '0.75To1' or wl == 'W.L.more1m':
            Ve = 1
    elif element_at_risk == 'infrastructure':
        if wl == 'less0.25':
            Ve = 0.25
        elif wl == '0.25To0.5':
            Ve = 0.75
        elif wl == '0.5To0.75' or wl == '0.75To1' or 'W.L.more1m':
            Ve = 1
    elif element_at_risk == 'rural_area':
        if wl == 'less0.25' or wl == '0.25To0.5':
            Ve = 0.5
        elif wl == '0.5To0.75' or wl == '0.75To1' or 'W.L.more1m':
            Ve = 1
    elif element_at_risk == 'natural_semi_natural_area':
        if wl == 'less0.25' or wl == '0.25To0.5':
            Ve = 0.25
        elif wl == '0.5To0.75' or wl == '0.75To1' or 'W.L.more1m':
            Ve = 0.5
    elif element_at_risk == 'Unknown':
        Ve = 0

    return Ve

#--------------------------------------------------------
# Calculate Vulnerability of people
def calculate_Vp(wl, exp_fld):
    # Calculate the DF
    if wl == 'less0.25':
        DF = 0
        wlv = 0.25
    elif wl == '0.25To0.5' or wl == '0.5To0.75':
        wlv = 0.75
        if exp_fld == 'rural_area':
            DF = 0
        elif exp_fld == 'natural_semi_natural_area':
            DF = 0.5
        else:
            DF = 1
    elif wl == '0.75To1' or wl == 'W.L.more1m':
        wlv = 1
        if exp_fld == 'rural_area':
            DF = 0.5
        elif exp_fld == 'natural_semi_natural_area':
            DF = 1
        else:
            DF = 1
    else:
        DF = 0
        wlv = 0.0

    FHR = 1.5 * wlv + DF

    if FHR < 0.75:
        Vp = 0.25
    elif 0.75 <= FHR < 1.25:
        Vp = 0.75
    elif FHR >= 1.25:
        Vp = 1

    if exp_fld == 'Unknown':
        Vp = 0

    # print("DF=", DF, " FHR=", FHR, "wlv=", wlv, 'Vp=', Vp)

    return (Vp)

#---------------------------------------------
def calculate_vulnerability(hf, Exp_fld):
    # To be change - for testing reasons
    Va = 1
    if len(Exp_fld) == 2:

        # Estimate Vp
        Vp = calculate_Vp(hf, Exp_fld[1])

        # Calculate Ve
        Ve = calculate_Ve(hf, Exp_fld[1])


        if Exp_fld[1] == 'Unknown':
            Va = 0

    elif len(Exp_fld) == 1:

       # No people in danger
       Vp = 0

       # Calculate Ve
       Ve = calculate_Ve(hf, Exp_fld[0])

       if Exp_fld[0] == 'Unknown':
           Va = 0

    return(Vp, Ve, Va)


#-----------------------------------------------------------------------------------------------------------
# Calculate the element at risk by extracting details from GIS (LandUse shapefile)
# regarding the elements at risk and people that are near to the Point of Interest
#
def find_element_at_risk_GIS(directory, geoserver_shapefile_name, PoI):

    # Extract the class of element at risk using the GIS file
    geo_resp_elem_at_risk = extract_ElementAtRisk_gis(geoserver_shapefile_name, PoI)
    len_geo_resp_elem_at_risk = len(geo_resp_elem_at_risk['features'])

    # write Geo-query reply in JSON to output file
    flname = directory + "/" + 'geoServer_LandUse_response_elem_at_risk.txt'
    with open(flname, 'w') as outfile:
        json.dump(OrderedDict(geo_resp_elem_at_risk), outfile)

    print(" No Features = ",len_geo_resp_elem_at_risk)

    # Find the appropriate polygon (or bbox) that point is belong to.
    # If more than one polygons are found then each polygon with its feature id
    # height and its category id are stored as a dictionary in an array.
    #
    count_true = 0
    count_false = 0

    Elements_at_risk = []

    Building_IDs_list = list([1,2,3,4,5,14,15,17,18,19,23,21,20,22])
    Infrastructure_IDs_list = list([12,13,16,50])
    Rural_area_IDs_list = list([6,7,61,62])
    Camp_Sport_IDs_list = list([11,8,10])
    Natural_IDs_list = list([9])

    if len_geo_resp_elem_at_risk > 0:

        for i in range(len(geo_resp_elem_at_risk['features'])):

            cand_bbox = geo_resp_elem_at_risk['features'][i]['properties']['bbox']
            cand_poly = geo_resp_elem_at_risk['features'][i]['geometry']['coordinates'][0][0]

            print("\n cand_poly = ", cand_poly)

            flagIsIns = point_in_polygon(PoI, cand_poly)

            if flagIsIns == True:
                print('Point IS inside in the feature with: ', geo_resp_elem_at_risk['features'][i]['id'])
                print('Properties= ', geo_resp_elem_at_risk['features'][i]['properties'])
                print(cand_bbox)

                class_elem_risk = geo_resp_elem_at_risk['features'][i]['properties']['CLASSE']

                if class_elem_risk in Building_IDs_list:
                    category = 'buildings'
                elif class_elem_risk in Infrastructure_IDs_list:
                    category = 'infrastructure'
                elif class_elem_risk in Rural_area_IDs_list:
                    category = 'rural_area'
                elif class_elem_risk in Camp_Sport_IDs_list:
                    category = 'camping_ground'
                elif class_elem_risk in Natural_IDs_list:
                    category = 'natural_semi_natural_area'

                item = {'Feature_Id': geo_resp_elem_at_risk['features'][i]['id'],
                        'Class': class_elem_risk,
                        'Categ_ElemOfRisk' : category
                        }

                Elements_at_risk.append(item)
                count_true += 1

            else:
                print('Point is not inside in the feature with: ', geo_resp_elem_at_risk['features'][i]['id'])
                count_false += 1

    # In case that no elements exists or the point is not inside into any polygon then...
    if len(Elements_at_risk) == 0:
        item = {'Feature_Id': "-",
                'Class': "-",
                'Categ_ElemOfRisk': 'Unknown'
                }

        Elements_at_risk.append(item)


    print(Elements_at_risk)

    return Elements_at_risk

# ===========================================================================================================
def calculate_exposure_from_GIS(directory, geoserver_shapefile_name, PoI):

    # Extract the class of element at risk using the GIS file
    geo_exposure = extract_ElementAtRisk_gis(geoserver_shapefile_name, PoI)
    len_geo_exposure = len(geo_exposure['features'])

    # write Geo-query reply in JSON to output file
    flname = directory + "/" + 'geoServer_Efinale_HMP_response_elem_at_risk.txt'
    with open(flname, 'w') as outfile:
        json.dump(OrderedDict(geo_exposure), outfile)

    print(" No Features = ", len_geo_exposure)
    Exposure_item = []

    if len_geo_exposure > 0:

        for i in range(len(geo_exposure['features'])):

            cand_bbox = geo_exposure['features'][i]['properties']['bbox']
            cand_poly = geo_exposure['features'][i]['geometry']['coordinates'][0][0]
            flagIsIns = point_in_polygon(PoI, cand_poly)

            if flagIsIns == True:
                print('Point IS inside in the feature with: ', geo_exposure['features'][i]['id'])
                print('Properties= ', geo_exposure['features'][i]['properties'])
                print(cand_bbox)

                item = {'Feature_Id': geo_exposure['features'][i]['id'],
                        'Ep': geo_exposure['features'][i]['properties']['ESPPERSONE'],
                        'Ee': geo_exposure['features'][i]['properties']['ESPECO'],
                        'Ea': geo_exposure['features'][i]['properties']['ESPAMB']
                        }
                Exposure_item.append(item)
            else:
                print('Point is not inside in the feature with: ', geo_exposure['features'][i]['id'])

    if len(Exposure_item) == 0:
        item = {'Feature_Id': "-", 'Ep': 0, 'Ee': 0, 'Ea': 0}
        Exposure_item.append(item)

    return Exposure_item

#-----------------------------------------------------------------------------------------------
# Estimate the Hydraulic Risk and its Severity

def calculate_Hydraulic_Risk( hazard_val, Vp, Ve, Va, Ep, Ee, Ea ):

    Hydraulic_Risk = hazard_val * (10*Ep*Vp + Ee*Ve + Ea*Va)/12

    # Estimate the risk severity
    if Hydraulic_Risk < 0.2:
        Severity = 'Minor' # 'Low'
    elif 0.2 <= Hydraulic_Risk < 0.5:
        Severity = 'Moderate' # 'Medium'
    elif 0.5 <= Hydraulic_Risk < 0.9:
        Severity = 'Severe' # 'High'
    elif 0.9 <= Hydraulic_Risk <= 1.0:
        Severity = 'Extreme' # 'Very_High'

    res = {'Hydraulic_Risk': round(Hydraulic_Risk, 2), 'Severity': Severity}

    return(res)

#---------------------------------------------------------------------------------------------------
# Estimate the Total Risk Assessment and its Severity by employing the Voting method
#
def calculate_risk_assessment( dset ):

    no_IncReports = len(dset['Hydraulic_Risk'])

    # Find the class distribution
    distr = dset.groupby('Severity').size()

    class_distrib = []
    # Distribution of incidents per risk category
    # The label item is needed only for use from the dashboard charts
    if 'Minor' in distr:
        class_distrib.append({'category': 'Minor', 'yValue': distr['Minor'], "color": "#31A34F", 'note':'Low' })
    else:
        class_distrib.append({'category': 'Minor', 'yValue': 0, "color": "#31A34F", 'note':'Low' })

    if 'Moderate' in distr:
        class_distrib.append({'category': 'Moderate', 'yValue': distr['Moderate'], "color": "#F8E71C", 'note':'Medium' })
    else:
        class_distrib.append({'category': 'Moderate', 'yValue': 0, "color": "#F8E71C", 'note':'Medium' })

    if 'Severe' in distr:
        class_distrib.append({'category': 'Severe', 'yValue': distr['Severe'], "color": "#E68431", 'note': 'High'})
    else:
        class_distrib.append({'category': 'Severe', 'yValue': 0, "color": "#E68431", 'note': 'High' })

    if 'Extreme' in distr:
        class_distrib.append({'category': 'Extreme', 'yValue': distr['Extreme'], "color": "#AA2050", 'note': 'Very High' })
    else:
        class_distrib.append({'category': 'Extreme', 'yValue': 0, "color": "#AA2050", 'note': 'Very High' })

    if 'Unknown' in distr:
        class_distrib.append({'category': 'Unknown', 'yValue': distr['Unknown'], 'color': "", 'note': ""})
    else:
        class_distrib.append({'category': 'Unknown', 'yValue': 0, 'color': "", 'note': ""})

    # Estimate the total risk and severity
    # severity = pd.Series(distr).idxmax()

    # Find all the categories with maximum distribution (frequency) [max value and position at class_distrib list of dictionaries]
    maxval = pd.Series(distr).max()
    # print(" maxval =",maxval)

    pos_max = [pos for pos, cldstr in enumerate(class_distrib) if cldstr['yValue'] == maxval]
    # print(pos_max)

    if len(pos_max) > 1:
        severity = class_distrib[pos_max[-1]]['category']
    else:
        severity = class_distrib[pos_max[0]]['category']

    print(" severity = ", severity)

    if severity == 'Minor':
        risk = 1/4*100 - 12.5
        category = 'Low'
    elif severity == 'Moderate':
        risk = 2/4*100 - 12.5
        category = 'Medium'
    elif severity == 'Severe':
        risk = 3/4*100 - 12.5
        category = 'High'
    elif severity == 'Extreme':
        risk = 4/4*100 - 12.5
        category = 'Very High'
    elif severity == 'Unknown':
        risk = 'Unknown'
        category = 'Unknown'

    total_risk_sever = { 'Total_Risk_Assessment': risk, 'Total_Severity': severity, 'Total_Category': category, 'Class_distribution': class_distrib }

    print("total_risk_sever = ", total_risk_sever)

    return( total_risk_sever )


#---------------------------------------------------------------------------------------------------
# Estimate the Total Risk Assessment and its Severity by employing the Generalised mean method
#
def calculate_risk_assessment_gm( dset, categories, power ):

    no_IncReports = len(dset['Hydraulic_Risk'])

    # Find the class distribution
    distr = dset.groupby('Severity').size()

    class_distrib = []
    # Distribution of incidents per risk category
    # The label item is needed only for use from the dashboard charts
    if 'Minor' in distr:
        class_distrib.append({'category': 'Minor', 'yValue': distr['Minor'], "color": "#31A34F", 'note':'Low' })
    else:
        class_distrib.append({'category': 'Minor', 'yValue': 0, "color": "#31A34F", 'note':'Low' })

    if 'Moderate' in distr:
        class_distrib.append({'category': 'Moderate', 'yValue': distr['Moderate'], "color": "#F8E71C", 'note':'Medium' })
    else:
        class_distrib.append({'category': 'Moderate', 'yValue': 0, "color": "#F8E71C", 'note':'Medium' })

    if 'Severe' in distr:
        class_distrib.append({'category': 'Severe', 'yValue': distr['Severe'], "color": "#E68431", 'note': 'High'})
    else:
        class_distrib.append({'category': 'Severe', 'yValue': 0, "color": "#E68431", 'note': 'High' })

    if 'Extreme' in distr:
        class_distrib.append({'category': 'Extreme', 'yValue': distr['Extreme'], "color": "#AA2050", 'note': 'Very High' })
    else:
        class_distrib.append({'category': 'Extreme', 'yValue': 0, "color": "#AA2050", 'note': 'Very High' })

    if 'Unknown' in distr:
        class_distrib.append({'category': 'Unknown', 'yValue': distr['Unknown'], 'color': "", 'note': ""})
    else:
        class_distrib.append({'category': 'Unknown', 'yValue': 0, 'color': "", 'note': ""})


    sum_distr = distr.sum()

    sum_distr_pow = 0
    for i, it in enumerate(class_distrib):
        if it['category'] == categories[i]['Class'] and it['category'] != 'Unknown':
            sum_distr_pow = sum_distr_pow + it['yValue']*categories[i]['Scale']**power

    severity_level = ceil(pow(sum_distr_pow/sum_distr, 1.0/power))
    print("severity_level=", severity_level)

    if severity_level == 1.0:
        severity = 'Minor'
        risk = 1/4*100 - 12.5
        category = 'Low'
    elif severity_level == 2.0:
        severity = 'Moderate'
        risk = 2/4*100 - 12.5
        category = 'Medium'
    elif severity_level == 3.0:
        severity = 'Severe'
        risk = 3/4*100 - 12.5
        category = 'High'
    elif severity_level >= 4.0:
        severity = 'Extreme'
        risk = 4/4*100 - 12.5
        category = 'Very High'
    else:
        severity = 'Unknown'
        risk = 'Unknown'
        category = 'Unknown'
      
    total_risk_sever = { 'Total_Risk_Assessment': risk, 'Total_Severity': severity, 'Total_Category': category, 'Class_distribution': class_distrib }

    return( total_risk_sever )


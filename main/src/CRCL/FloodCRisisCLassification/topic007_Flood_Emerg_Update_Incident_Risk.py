import json
import os, errno
from pathlib import Path
from pandas import read_csv, DataFrame, concat, ExcelWriter
from datetime import datetime, timedelta
from collections import OrderedDict

from CRCL.FloodCRisisCLassification.Topic007_Update_Incident_Risk import Top007_Update_Incident_Risk


#------------------------------------------------------------------------------------------------
# Sending message to KB for update the severity of an incident report
#
def topic007Flood_Update_Incident_Risk(directory, IncRep_dict, IncDescr, producer):

     # Set variables for the header of the message
    district = "Vicenza"
    sender = "CRCL"
    specificSender = ""

    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")

     # Current datetime that message sends to KB (in UTC)
    sentUTC = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'

    status = "Actual"
    actionType = "Update"
    scope = "Public"
    code = '0'
    references = ""
    note = ""
    recipients = ""

    # Set variables for the body of the message
    lang = "en-US"
    incidentOrigin = 'CRCL'

    # incident id = IncRep_dict['Incident_ID']
    incidentID = str( IncRep_dict['Incident_ID'] )

    # Position of the Weather Station
    position = [ IncRep_dict['Inc_Long'], IncRep_dict['Inc_Lat'] ]

    incidentDateTimeUTC = IncRep_dict['Incident_DateTime']

    msgTitle = "Update Risk/Severity level"

    # inc_description = "Update Severity level of Incident message from citizens/first responders MobileApp with ID:" + str(IncRep_dict['Incident_ID'])
    if IncDescr == 'None':
         inc_description = "Updated Severity Level from the Application!"
    else:
        inc_description = IncDescr


    IncidentProperties = {'Hazard': IncRep_dict['Hazard_field'],
                          'Exposure': IncRep_dict['Exposure_field'],
                          'Category': IncRep_dict['Category_field'] }

    Incident_RiskAssessment = { 'Hazard_value': IncRep_dict['Hazard_value'],
                                'Exposure_People': IncRep_dict['Exposure_people'],
                                'Exposure_Economic_Activities': IncRep_dict['Exposure_economic_act'],
                                'Exposure_Cultural_Asset': IncRep_dict['Exposure_cultural_asset'],
                                'Vulnerability_People': IncRep_dict['Vulnerability_people'],
                                'Vulnerability_Economic_Activities': IncRep_dict['Vulnerability_economic_act'],
                                'Vulnerability_Cultural_Asset': IncRep_dict['Vulnerability_cultural_asset'],
                                'Risk': IncRep_dict['Hydraulic_Risk']
                            }

    Incident_Severity = IncRep_dict['Severity']

    top007_update_risk_IR = Top007_Update_Incident_Risk( msgIdent, sender, specificSender, sentUTC, district, code, note, recipients,
                                                         lang, references, msgTitle, incidentOrigin, incidentID, incidentDateTimeUTC,
                                                         inc_description, position, IncidentProperties, Incident_RiskAssessment,
                                                         Incident_Severity)

    # call class functions

    # Create the header of the object (message)
    top007_update_risk_IR.create_dictHeader_top007()

    # create the body of the object
    top007_update_risk_IR.create_dictBody_top007()

    # create the Top007_Update_Incident_Risk as json
    top007_UpdateSeverityIR = OrderedDict()
    top007_UpdateSeverityIR['header']= top007_update_risk_IR.header
    top007_UpdateSeverityIR['body']= top007_update_risk_IR.body

    # write json (top007_UpdateSeverityIR) to output file
    flname = directory + "/" + 'TOP007_Update_Incident_Risk' + '_' + 'IncidentID' + '_' + str(IncRep_dict['Incident_ID']) + \
            '_' + 'IncRep_DateTime' + '_' + str(IncRep_dict['Incident_DateTime']) + '.txt'

    with open(flname, 'w') as outfile:
        json.dump(top007_UpdateSeverityIR, outfile, indent=4)

    print('Messages TOP007 for Update_Incident_Risk with ID: ', IncRep_dict['Incident_ID'], ' have been forwarded to logger!')
    producer.send("TOP007_UPDATE_INCIDENT_RISK", top007_UpdateSeverityIR)


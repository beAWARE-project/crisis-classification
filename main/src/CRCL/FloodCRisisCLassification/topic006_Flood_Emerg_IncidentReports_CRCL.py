import json, time, re
import os, errno
from pathlib import Path
from pandas import read_csv, DataFrame, concat, ExcelWriter
from datetime import datetime, timedelta
from math import pow, ceil
from collections import OrderedDict

from uuid import *

from CRCL.FloodCRisisCLassification.Topic006_Incident_Report_CRLC import *
from CRCL.FloodCRisisCLassification.Auxiliary_functions import *
from bus.bus_producer import BusProducer

#-----------------------------------------------------------------------------------------------------------------------
#
# Topic 006 for Observed Precipitation Measurements for Weather Station where its current observed precipitation value exceeds
# the predefined 1st Alarm Threshold. The results are going to present to the Map as Incident Report
#
def topic006_FloodEmerg_Precipitation_IR(directory, PRDS_item, producer):

    # Set variables for the header of the message
    district = "Vicenza"
    sender = "CRCL"
    specificSender = ""

    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")
    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'

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

    # incident id = ID_WS + ID_DataStream + ID current measurement (IoT_ID)
    # incidentID = str(PRDS_item['ID']) + "_" + str(PRDS_item['DS_ID']) + "_" + str(PRDS_item['IoT_ID'][-1])
    val_ID = str( uuid4() ).replace("-","")
    incidentID = str(PRDS_item['ID']) + "_" + str(PRDS_item['DS_ID']) + "_" + str(val_ID)

    incidentType = "Overflow"

    # Position of the Weather Station
    position = [PRDS_item['Coordinates'][0], PRDS_item['Coordinates'][1]]

    start_dateTime = PRDS_item['PhenDateTime'][-1]

    inc_title = "W.S.: " + PRDS_item['WS_name'] + "," + PRDS_item['Note'][-1].split("exceeds")[1]

    # Calculate the SeverityValue by the Note_Scale
    if PRDS_item['Note_Scale'][-1] == 'Low':
        SeverityValue = 'Minor'
    elif PRDS_item['Note_Scale'][-1] == 'Medium':
        SeverityValue = 'Moderate'
    elif PRDS_item['Note_Scale'][-1] == 'High':
        SeverityValue = 'Severe'
    elif PRDS_item['Note_Scale'][-1] == 'Very High':
        SeverityValue = 'Extreme'

    # ", ID: " + str(PRDS_item['ID']) +
    inc_description = "Weather Station: " + PRDS_item['WS_name'] + "," + " Severity: " + SeverityValue + \
                      "," + " Precipitation value: " + str(PRDS_item['Precipitation'][-1])

    attachments = []

    dataStrmID = "FLCR_1122_OPRm"
    dataSerID = str(PRDS_item['ID']) + "_" + str(PRDS_item['DS_ID'])

    top006_incRep_PR = Top006_Incident_Report_CRCL(msgIdent, sender, specificSender, sent_dateTime, district, code, note, recipients,
                                              lang, references, inc_title, incidentOrigin, incidentID, incidentType, start_dateTime,
                                              inc_description, position, attachments, dataStrmID, dataSerID, SeverityValue)


    # call class function
    # Create the header of the object (message)
    top006_incRep_PR.create_dictHeader_top006()

    # create the body of the object
    top006_incRep_PR.create_dictBody_top006()

    # create the TOP006_INCIDENT_REPORT_CRCL as json
    top006_PRIR = OrderedDict()
    top006_PRIR['header']= top006_incRep_PR.header
    top006_PRIR['body']= top006_incRep_PR.body

    # write json (top21_PR) to output file
    flname = directory + "/" + 'TOP006_Precipitation_IncidentReport' + '_' + 'WeatherStation' + '_' + str(PRDS_item['ID']) + \
            '_' + PRDS_item['WS_name'] + '.txt'

    with open(flname, 'w') as outfile:
        json.dump(top006_PRIR, outfile, indent=4)

    print('Messages TOP006 for Precipitation over the Weather Station with name: ', PRDS_item['WS_name'], ' have been forwarded to logger!')
    producer.send("TOP006_INCIDENT_REPORT_CRCL", top006_PRIR)



# -----------------------------------------------------------------------------------------------------------------------
#
# Topic 006 for Observed Water Level Measurements for Weather Station where its current observed WL value exceeds
# the predefined 1st Alarm Threshold. The results are going to present to the Map as Incident Report
#
def topic006_FloodEmerg_WaterLevel_IR(directory, WLDS_item, producer):
    # Set variables for the header of the message
    district = "Vicenza"
    sender = "CRCL"
    specificSender = ""

    msgIdent = datetime.utcnow().isoformat().replace(":", "").replace("-", "").replace(".", "MS")
    sent_dateTime = datetime.utcnow().replace(microsecond=0).isoformat() + 'Z'

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

    # incident id = ID_WS + ID_DataStream + ID current measurement (IoT_ID)
    # incidentID = str(WLDS_item['ID']) + "_" + str(WLDS_item['DS_ID']) + "_" + str(WLDS_item['IoT_ID'][-1])

    val_ID = str( uuid4() ).replace("-","")
    incidentID = str(WLDS_item['ID']) + "_" + str(WLDS_item['DS_ID']) + "_" + str(val_ID)
    incidentType = "Overflow"

    # Position of the Weather Station
    position = [WLDS_item['Coordinates'][0], WLDS_item['Coordinates'][1]]

    start_dateTime = WLDS_item['PhenDateTime'][-1]

    inc_title = "W.S.: " + WLDS_item['WS_name'] + "," + WLDS_item['Note'][-1].split("exceeds")[1]

    # Calculate the SeverityValue by the Note_Scale
    if WLDS_item['Note_Scale'][-1] == 'Low':
        SeverityValue = 'Minor'
    elif WLDS_item['Note_Scale'][-1] == 'Medium':
        SeverityValue = 'Moderate'
    elif WLDS_item['Note_Scale'][-1] == 'High':
        SeverityValue = 'Severe'
    elif WLDS_item['Note_Scale'][-1] == 'Very High':
        SeverityValue = 'Extreme'


    # ", ID: " + str(WLDS_item['ID']) +
    inc_description = "Weather Station: " + WLDS_item['WS_name'] + "," + "Severity: " + SeverityValue + \
                      "," + " Water Level value: " + str(WLDS_item['Water_level'][-1])

    attachments = []

    dataStrmID = "FLCR_1112_OWLm"
    dataSerID = str(WLDS_item['ID']) + "_" + str(WLDS_item['DS_ID'])

    top006_incRep_WL = Top006_Incident_Report_CRCL(msgIdent, sender, specificSender, sent_dateTime, district, code, note,
                                              recipients, lang, references, inc_title, incidentOrigin, incidentID, incidentType,
                                              start_dateTime, inc_description, position, attachments, dataStrmID, dataSerID, SeverityValue)

    # call class function
    # Create the header of the object (message)
    top006_incRep_WL.create_dictHeader_top006()

    # create the body of the object
    top006_incRep_WL.create_dictBody_top006()

    # create the TOP006_INCIDENT_REPORT_CRCL as json
    top006_WLIR = OrderedDict()
    top006_WLIR['header']= top006_incRep_WL.header
    top006_WLIR['body']= top006_incRep_WL.body

    # write json (top006_WLIR) to output file
    flname = directory + "/" + 'TOP006_WaterLevel_IncidentReport' + '_' + 'WeatherStation' + '_' + str(WLDS_item['ID']) + '_' + WLDS_item['WS_name'] + '.txt'

    with open(flname, 'w') as outfile:
        json.dump(top006_WLIR, outfile, indent=4)

    print('Messages TOP006 for Water Level over the Weather Station with name: ', WLDS_item['WS_name'],
          ' have been forwarded to logger!')
    producer.send("TOP006_INCIDENT_REPORT_CRCL", top006_WLIR)

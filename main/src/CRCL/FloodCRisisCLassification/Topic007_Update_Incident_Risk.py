from collections import OrderedDict

class Top007_Update_Incident_Risk:
    'Class for manipulation (create, edit, delete) Top007_Update_Incident_Risk'


    # Constractor of the class
    def __init__(self, msgIdentifier, sender, specificSender, sentUTC, district, code, note, recipients,
                 language, references, msgTitle, incidentOriginator, incidentID, incidentDateTimeUTC, description, position,
                 IncidentProperties, Incident_RiskAssessment, Incident_Severity):

        # header variables
        self.topic_topicName = "TOP007_UPDATE_INCIDENT_RISK"
        self.topic_topicMajorVersion = 0
        self.topic_topicMinorVersion = 1
        self.topic_sender = sender
        self.topic_msgIdentifier =  msgIdentifier
        self.topic_sentUTC = sentUTC
        self.topic_status = "Actual"
        self.topic_actionType = "Alert"
        self.topic_specificSender = specificSender
        self.topic_scope = "Restricted"
        self.topic_district = district
        self.topic_recipients = recipients
        self.topic_code = code
        self.topic_note = note
        self.topic_references = references
        # body variables
        self.topic_title = msgTitle
        self.topic_incidentOriginator = incidentOriginator
        self.topic_incidentID = incidentID
        self.topic_incidentDateTimeUTC = incidentDateTimeUTC
        self.topic_language = language
        self.topic_description = description
        self.topic_position = {"longitude": position[0], "latitude": position[1] }

        # Dictionary which contains the incident characteristics such as:
        # Hazard_field, Exposure_field and Category_field
        self.topic_IncProperties = IncidentProperties

        # Dictionary which contains the results of the risk assessment process of an
        # incident report (Vulnerability, Hazard, Exposure, Risk assessment metrics)
        self.topic_IncRiskAssesmentValues = Incident_RiskAssessment

        self.topic_Severity = Incident_Severity

    # Create the header of the class object
    def create_dictHeader_top007(self):
        self.header = OrderedDict()

        self.header["topicName"]= self.topic_topicName
        self.header["topicMajorVersion"]= self.topic_topicMajorVersion
        self.header["topicMinorVersion"]= self.topic_topicMinorVersion
        self.header["sender"]= self.topic_sender
        self.header["msgIdentifier"]= self.topic_msgIdentifier
        self.header["sentUTC"]= self.topic_sentUTC
        self.header["status"]= self.topic_status
        self.header["actionType"]= self.topic_actionType
        self.header["specificSender"]= self.topic_specificSender
        self.header["scope"]= self.topic_scope
        self.header["district"]= self.topic_district
        self.header["recipients"]= self.topic_recipients
        self.header["code"]= self.topic_code
        self.header["note"]= self.topic_note
        self.header["references"]= self.topic_references

    # Create the body of the class object
    def create_dictBody_top007(self):
        self.body = OrderedDict()

        self.body["title"]= self.topic_title
        self.body["incidentOriginator"]=  self.topic_incidentOriginator
        self.body["incidentID"]= self.topic_incidentID
        self.body["incidentDateTimeUTC"]= self.topic_incidentDateTimeUTC
        self.body["language"]= self.topic_language
        self.body["description"]= self.topic_description
        self.body["position"] = {"longitude": self.topic_position["longitude"],
                                     "latitude": self.topic_position["latitude"]}
        self.body["IncidentProperties"] = self.topic_IncProperties
        self.body["Incident_RiskAssessment"]= self.topic_IncRiskAssesmentValues
        self.body["severity"] = self.topic_Severity


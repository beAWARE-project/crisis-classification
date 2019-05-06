from builtins import zip
from collections import OrderedDict


class TOP106_Risk_Maps:
    'Class for manipulation (create, edit, delete) Topic 106_Risk_Maps'

    # Constractor of the class
    def __init__(self, msgIdentifier, sentUTC, status, actionType, scope, district, code,
                 dataStreamGenerator, dataStreamID, dataStreamName, dataStreamDescription, language,
                 dataStreamCategory, dataStreamSubCategory, position, Polygon_list):
        # header variables
        self.topic_name = "TOP106_RISK_MAPS"
        self.topic_MajorVersion = '0'
        self.topic_MinorVersion = '1'
        self.topic_sender = "CRCL"
        self.topic_msgIdentifier = msgIdentifier
        self.topic_sentUTC = sentUTC
        self.topic_status = status
        self.topic_actionType = actionType
        self.topic_specificSender = ""
        self.topic_scope = scope
        self.topic_district = district
        self.topic_recipients = ""
        self.topic_code = code
        self.topic_note = ""
        self.topic_references = ""

        # body variables
        self.topic_dataStreamGenerator = dataStreamGenerator
        self.topic_dataStreamID = dataStreamID
        self.topic_dataStreamName = dataStreamName
        self.topic_dataStreamDescription = dataStreamDescription
        self.topic_language = language
        self.topic_dataStreamCategory = dataStreamCategory
        self.topic_dataStreamSubCategory = dataStreamSubCategory
        self.topic_position = {"longitude": position['longitude'], "latitude": position['latitude']}
        self.polygons = Polygon_list

    # Create the header of the class object
    def create_dictHeader(self):
        self.header = OrderedDict()

        self.header["topicName"] = self.topic_name
        self.header["topicMajorVersion"] = self.topic_MajorVersion
        self.header["topicMinorVersion"] = self.topic_MinorVersion
        self.header["sender"] = self.topic_sender
        self.header["msgIdentifier"] = self.topic_msgIdentifier
        self.header["sentUTC"] = self.topic_sentUTC
        self.header["status"] = self.topic_status
        self.header["actionType"] = self.topic_actionType
        self.header["specificSender"] = self.topic_specificSender
        self.header["scope"] = self.topic_scope
        self.header["district"] = self.topic_district
        self.header["recipients"] = self.topic_recipients
        self.header["code"] = self.topic_code
        self.header["note"] = self.topic_note
        self.header["references"] = self.topic_references

    # Create the body of the class object
    def create_dictBody(self):
        self.body = OrderedDict()

        self.body["dataStreamGenerator"] = self.topic_dataStreamGenerator
        self.body["dataStreamID"] = self.topic_dataStreamID
        self.body["dataStreamName"] = self.topic_dataStreamName
        self.body["dataStreamDescription"] = self.topic_dataStreamDescription
        self.body["language"] = self.topic_language
        self.body["dataStreamCategory"] = self.topic_dataStreamCategory
        self.body["dataStreamSubCategory"] = self.topic_dataStreamSubCategory
        self.body['position'] = self.topic_position
        self.body["polygons"] = self.polygons
from builtins import zip
from collections import OrderedDict


class Top104_Metric_Report:
    'Class for manipulation (create, edit, delete) Topic 104 Metric Report'

    # Constractor of the class
    def __init__(self, msgIdentifier, sentUTC, status, actionType, scope, district, code,
                 dataStreamGenerator, dataStreamID, dataStreamName, dataStreamDescription, language,
                 dataStreamCategory, dataStreamSubCategory, position):
        # header variables
        self.topic_name = "TOP104_METRIC_REPORT"
        self.topic_MajorVersion = '0'
        self.topic_MinorVersion = '1'
        self.topic_sender = "CRCL"
        self.topic_msgIdentifier = msgIdentifier
        self.topic_sentUTC = sentUTC
        self.topic_status = status
        self.topic_actionType = actionType
        self.topic_scope = scope
        self.topic_district = district
        self.topic_code = code
        self.topic_references = ""
        self.topic_note = ""
        self.topic_specificSender = ""
        self.topic_recipients = ""
        # body variables
        self.topic_dataStreamGenerator = dataStreamGenerator
        self.topic_dataStreamID = dataStreamID
        self.topic_dataStreamName = dataStreamName
        self.topic_dataStreamDescription = dataStreamDescription
        self.topic_language = language
        self.topic_dataStreamCategory = dataStreamCategory
        self.topic_dataStreamSubCategory = dataStreamSubCategory
        self.topic_position = {"longitude": position[0], "latitude": position[1] }
        # measurement variables
        self.topic_measurementID = []
        self.topic_measurementTimeStamp = []
        self.topic_dataSeriesID = []
        self.topic_dataSeriesName = []
        self.topic_xValue = []
        self.topic_yValue = []
        self.topic_meas_color = []
        self.topic_meas_note = []
        self.topic_meas_category = []
        self.header = None
        self.body = None
        self.measurements = []

        # threshold variables
        self.topic_thresh_note = []
        self.topic_thresh_color = []
        self.topic_thresh_xValue = []
        self.topic_thresh_yValue = []
        self.thresholds = []


    # Create the header of the class object
    def create_dictHeader(self):
        self.header = OrderedDict()

        self.header["topicName"]= self.topic_name
        self.header["topicMajorVersion"]= self.topic_MajorVersion
        self.header["topicMinorVersion"]= self.topic_MinorVersion
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
    def create_dictBody(self):
        self.body = OrderedDict()

        self.body["dataStreamGenerator"]= self.topic_dataStreamGenerator
        self.body["dataStreamID"]= self.topic_dataStreamID
        self.body["dataStreamName"]= self.topic_dataStreamName
        self.body["dataStreamDescription"]= self.topic_dataStreamDescription
        self.body["language"]= self.topic_language
        self.body["dataStreamCategory"]= self.topic_dataStreamCategory
        self.body["dataStreamSubCategory"]= self.topic_dataStreamSubCategory
        self.body["measurements"]= self.measurements

    # Create the body of the class object with the Thresholds
    def create_dictBody_withThresh(self):
        self.body = OrderedDict()

        self.body["dataStreamGenerator"] = self.topic_dataStreamGenerator
        self.body["dataStreamID"] = self.topic_dataStreamID
        self.body["dataStreamName"] = self.topic_dataStreamName
        self.body["dataStreamDescription"] = self.topic_dataStreamDescription
        self.body["language"] = self.topic_language
        self.body["dataStreamCategory"] = self.topic_dataStreamCategory
        self.body["dataStreamSubCategory"] = self.topic_dataStreamSubCategory
        self.body["measurements"] = self.measurements

        item = {"id": self.topic_dataSeriesID[0], "threshold": self.thresholds}
        self.body["thresholds"]= { "dataSeriesIDThresholds": [item] }

    # Create the list of measurements for the class object
    def create_dictMeasurements_Categ(self):
        for (cid, ctime, cx, cy, ccol, cnote, ccateg, dsID,dsN) in zip(self.topic_measurementID,
                                                     self.topic_measurementTimeStamp,
                                                     self.topic_xValue,
                                                     self.topic_yValue,
                                                     self.topic_meas_color,
                                                     self.topic_meas_note,
                                                     self.topic_meas_category,
                                                     self.topic_dataSeriesID,
                                                     self.topic_dataSeriesName):
            temp_meas = OrderedDict()
            temp_meas["measurementID"]= cid
            temp_meas["measurementTimeStamp"]= ctime
            temp_meas["dataSeriesID"]= dsID
            temp_meas["dataSeriesName"]= dsN
            temp_meas["position"] = {"longitude": self.topic_position["longitude"],
                                     "latitude": self.topic_position["latitude"]}
            temp_meas["xValue"] = cx
            temp_meas["yValue"] = cy
            temp_meas["color"] = ccol
            temp_meas["note"] = cnote
            temp_meas["category"] = ccateg

            self.measurements += [temp_meas]

    # Create the list of measurements without category for the class object
    def create_dictMeasurements(self):
        for (cid, ctime, cx, cy, ccol, cnote, dsID,dsN) in zip(self.topic_measurementID,
                                                     self.topic_measurementTimeStamp,
                                                     self.topic_xValue,
                                                     self.topic_yValue,
                                                     self.topic_meas_color,
                                                     self.topic_meas_note,
                                                     self.topic_dataSeriesID,
                                                     self.topic_dataSeriesName):
            temp_meas = OrderedDict()
            temp_meas["measurementID"]= cid
            temp_meas["measurementTimeStamp"]= ctime
            temp_meas["dataSeriesID"]= dsID
            temp_meas["dataSeriesName"]= dsN
            temp_meas["position"] = {"longitude": self.topic_position["longitude"],
                                     "latitude": self.topic_position["latitude"]}
            temp_meas["xValue"] = cx
            temp_meas["yValue"] = cy
            temp_meas["color"] = ccol
            temp_meas["note"] = cnote

            self.measurements += [temp_meas]

    #--------------------------------------------------------------
    # Create the list of thresholds
    def create_dictThresholds(self):

        for (cnote, cx, cy, ccol) in zip(self.topic_thresh_note,
                                         self.topic_thresh_xValue,
                                         self.topic_thresh_yValue,
                                         self.topic_thresh_color):

            temp_thresh = OrderedDict()
            temp_thresh["note"] = cnote
            temp_thresh["xValue"] = cx
            temp_thresh["yValue"] = cy
            temp_thresh["color"] = ccol

            self.thresholds += [temp_thresh]





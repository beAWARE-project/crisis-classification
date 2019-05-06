# -----------------------------------------------------------------------------------------------------
# 2nd PROTOTYPE VERSION based on:
#   - VICENZA PILOT for the Flood,
#   - THESSALONIKI PILOT for the HeatWave (1st Prototype)
#

from confluent_kafka import Consumer
import json
import asyncio
import sqlite3
from threading import Thread
from bus.bus_producer import BusProducer

import time

# Flood pilot, versions with MySQL DataBase
# Pre-Emergency phase
from CRCL.FloodCRisisCLassification.CRCL_FLOOD_PreEmerg_2nd_Prototype import *
from CRCL.FloodCRisisCLassification.CRCL_FLOOD_RealData_PreEmerg_2nd_Prototype import *
# Emergency phase
from CRCL.FloodCRisisCLassification.CRCL_FLOOD_Emerg_2nd_Prototype import *
from CRCL.FloodCRisisCLassification.CRCL_FLOOD_RealData_Emerg_2nd_Prototype import *
# Emergency Phase utilising Incident Reports
from CRCL.FloodCRisisCLassification.CRCL_FLOOD_Emerg_IncidentRep_2nd_Prototype import *

#--------------------------------------------
# Heatwave Pilot
# Pre-Emergency phase
from CRCL.HeatwaveCRisisCLassification.HeatWaveCRCL_RealData_PreEmerg_2nd_Prototype import *
from CRCL.HeatwaveCRisisCLassification.HeatWaveCRCL_FakeData_PreEmerg_2nd_Prototype import *
# Emergency phase
from CRCL.HeatwaveCRisisCLassification.HeatWaveCRCL_FakeData_Emerg_2nd_Prototype import *
from CRCL.HeatwaveCRisisCLassification.HeatWaveCRCL_RealData_Emerg_2nd_Ptototype import *

#--------------------------------------------
# Fire Pilot
# Pre-Emergency phase
from CRCL.FireCRisisCLassification.CRCL_FIRE_PreEmerg_2nd_Prototype import *


#------------------------------------------------------------------------------------------------------
class BusConsumer:
    def __init__(self):

        # Pre-shared credentials
        self.credentials = json.load(open('bus_credentials.json'))

        # Construct required configuration
        self.configuration = {
            'client.id': 'CRCL_consumer',
            'group.id': 'CRCL_consumer_group',
            'bootstrap.servers': ','.join(self.credentials['kafka_brokers_sasl']),
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': '/etc/ssl/certs',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.credentials['api_key'][0:16],
            'sasl.password': self.credentials['api_key'][16:48],
            'api.version.request': True
        }

        self.consumer = Consumer(self.configuration)

        self.listening = True

        self.database = 'messages.sqlite'

    def listen(self, topics):
        # Topics should be a list of topic names e.g. ['topic1', 'topic2']

        self.listening = True

        # Subscribe to topics
        try:
            self.consumer.subscribe(topics)
        except Exception as e:
            print(e)
            return False

        # Initiate a loop for continuous listening
        while self.listening:
            msg = self.consumer.poll(0)

            # If a message is received and it is not an error message
            if msg is not None and msg.error() is None:
                # print('Message consumed: topic={0}, partition={1}, offset={2}, key={3}, value={4}'.format(
                #     msg.topic(),
                #     msg.partition(),
                #     msg.offset(),
                #     msg.key().decode('utf-8'),
                #     msg.value().decode('utf-8')))

                # print('Message consumed: topic={0}, partition={1}, offset={2}, key={3}'.format(
                #     msg.topic(),
                #     msg.partition(),
                #     msg.offset(),
                #     msg.key().decode('utf-8')))
                print("\n\n ***********************************************************\n ")
                print("RECEIVED: " + msg.topic())
                Topic_Type = msg.topic()

                # Add incoming message to requests database
                try:
                    message_text = msg.value().decode('utf-8')
                except:
                    message_text = msg.value()

                # self.submit_message_to_database(message_text)
                print("\n INSIDE METHOD listen \n")
                # print(len(message_text))

                json_msg = json.loads(message_text)
                # self.submit_message_to_crcl(json_msg)

                # Runs automatically when it receives a message
                t = Thread(target=self.submit_message_to_crcl, args=(json_msg, Topic_Type,))
                t.start()
            # Sleep for a while
            # asyncio.sleep(0.43)

        # Unsubscribe and close consumer
        self.consumer.unsubscribe()
        self.consumer.close()

    def stop(self):
        self.listening = False

    def submit_message_to_database(self, message):

        try:
            con = sqlite3.connect(self.database)

            with con:
                cur = con.cursor()
                cur.execute('INSERT INTO requests (message) VALUES (?)', (message,))

            cur.close()

        except sqlite3.Error as e:
            print("Error %s:" % e.args[0])
            return False

        # con = sqlite3.connect(self.database)
        #
        # with con:
        #     cur = con.cursor()
        #     cur.execute('INSERT INTO requests (message) VALUES (?)', (message,))
        #
        # cur.close()


    #==================================================================================
    def submit_message_to_crcl(self, json_msg, Topic_Type):

        import time

        print(" \n INSIDE submit_message_to_crcl \n")
        current_date = datetime.utcnow().replace(microsecond=0)

        # Check if its TOP21 or TOP105
        if Topic_Type == 'TOP105DEV_CRCL_INITIALIZATION':

            print(type(json_msg))
            flag_mode = json_msg['body']['flagMode']
            print(flag_mode)

            if json_msg['body']['TaskID'] == "1001":

                # For Pilot Vicenza
                # Session = Dataset number
                if flag_mode == 'Fake':

                    session = json_msg['body']['session']

                    print('\n ===>>>> Call Pre-Emerg method for: ', flag_mode, 'and Session:', session )
                    for s_it in range(len(session)):
                        print("\n ************ \n Current Run at date/time: ", current_date)
                        print(" ====>>> Execute Session : ", session[s_it])

                        CrisisClassificationFlood_PreEmerg_v2(current_date, flag_mode, session[s_it])

                elif flag_mode == 'Real-Time':

                    # if time_mode = '2010', then old flood data is retrieved. flag_AMICO_run should be FALSE!!!
                    time_mode = json_msg['body']['timeMode']

                    if time_mode != 'Current' and time_mode != '2010':
                        time_mode = 'Current'

                    # if flag_AMICO_run = True then the lastRun is executed ignoring the Date/Time interval
                    if time_mode == '2010':
                        flag_AMICO_run = False
                    elif time_mode == 'Current':
                        flag_AMICO_run = json_msg['body']['flag_AMICO_run']

                    print('Call method for: ', flag_mode, " and for ", time_mode, " flag_AMICO_run = ", flag_AMICO_run )

                    CrisisClassificationFlood_PreEmerg_RealData_v2(current_date, flag_mode, time_mode, flag_AMICO_run)

            elif json_msg['body']['TaskID'] == "1002":

                if flag_mode == 'Fake':

                    # Session = Dataset number
                    # Fake
                    session = json_msg['body']['session']
                    print('====>>>>> Call Emergency method for: ', flag_mode, ' and session:', session)

                    for s_it in range(len(session)):
                        print("\n ************ \n Current Run at date/time: ", current_date)
                        print(" ====>>> Execute Session : ", session[s_it])

                        CrisisClassificationFlood_Emerg_v2(current_date, flag_mode, session[s_it])

                elif flag_mode == 'Real-Time':

                    print('Call method for: ', flag_mode)
                    # Real time data
                    CrisisClassificationFlood_RealData_Emerg_v2(current_date, flag_mode)

            # ------------------------------------------------------------------------
            # FIRE PILOT
            elif json_msg['body']['TaskID'] == "2001":
                import time
                millis = int(round(time.time() * 1000))
                print(millis)

                if flag_mode == 'Real-Time':
                    CrisisClassificationFire_PreEmerg_v2()
                else:
                    print(" Please provide the correct value to the flag_mode item. ")

            # -----------------------------------------------------------------------
            # HEATWARE PILOT
            elif json_msg['body']['TaskID'] == "3001":

                print(" Pre-Emergency HEATWAVE 3001!!!")

                if 'Category_Threshold' in json_msg['body']:
                    CategThresh = json_msg['body']['Category_Threshold']
                    print('CategThresh=', CategThresh)
                else:
                    CategThresh = 'More than half population feels discomfort'
                    print('CategThresh=', CategThresh)

                iteration = 1

                # Obtain new forecasts every 3h periodically for duration 3 days
                step = timedelta(hours=3)
                end = current_date + timedelta(days=3)

                # Indicates the status of the locations and the overall RoI (Thessaloniki).
                # If a location has problem (the DI exceeds a pre-defined threshold),
                # then a TOP104 will be created and the flag of the status will change to True.
                # When the DI value become lower that the threshold, then flag of the status will
                # change to False sending a new message to inform PSAP/Dashboard that it is changed.
                #
                if iteration <= 1:
                    # Initialize flags ...else keep the current status
                    #
                    flag_location_status = [{'name': 'Euosmos', 'flag_Status_1st': False, 'flag_Status_max': False},
                                            {'name': 'Aristotelous Sq.', 'flag_Status_1st': False,
                                             'flag_Status_max': False},
                                            {'name': 'Faliro', 'flag_Status_1st': False, 'flag_Status_max': False},
                                            {'name': 'Konstantinopolitika', 'flag_Status_1st': False,
                                             'flag_Status_max': False},
                                            {'name': 'Thermi-Xortiatis', 'flag_Status_1st': False,
                                             'flag_Status_max': False},
                                            {'name': 'Airport', 'flag_Status_1st': False, 'flag_Status_max': False}]

                    flag_HOCL = [{'name': 'OHWCL_Day_1', 'flag_Status': False},
                                 {'name': 'OHWCL_Day_2', 'flag_Status': False},
                                 {'name': 'OHWCL_Day_3', 'flag_Status': False}
                                 ]

                # Open files to store the flags
                flname = 'response_HW_PreEmerg_FLAGS' + '_' + flag_mode + '.txt'
                outfl = open(flname, 'w')

                if flag_mode == 'Real-Time':
                    # Real Time Data from Queries
                    CrisisClassificationHeatwave_RealData_PreEmerg_v2(iteration, flag_location_status, flag_HOCL, CategThresh)

                elif flag_mode == 'Fake':
                    # Fake Data from excel
                    CrisisClassificationHeatwave_FakeData_PreEmerg_v2(iteration, flag_location_status, flag_HOCL, CategThresh)

                outfl.close()

            # -----------------------------------------------------------------------
            elif json_msg['body']['TaskID'] == "3002":
                print(" Emergency Heatwave Crisis 3002 !!!")

                if 'Category_Threshold' in json_msg['body']:
                    CategThresh = json_msg['body']['Category_Threshold']
                    print('CategThresh=', CategThresh)
                else:
                    CategThresh = 'More than half population feels discomfort'
                    print('CategThresh=', CategThresh)

                iteration = 1

                # Obtain new real-time observations every 3 minutes periodically
                # for duration 3 days
                step = timedelta(minutes=3)
                end = current_date + timedelta(days=3)

                # Indicates the status of the locations and the overall RoI (Thessaloniki).
                # If a location has problem (the DI exceeds a pre-defined threshold),
                # then a TOP104 will be created and the flag of the status will change to True.
                # When the DI value become lower that the threshold, then flag of the status will
                # change to False sending a new message to inform PSAP/Dashboard that it is changed.
                #
                if iteration <= 1:
                    # Initialize flags ...else keep the current status
                    #
                    flag_location_status = [{'name': 'Euosmos', 'flag_Status': False},
                                            {'name': 'Aristotelous Sq.', 'flag_Status': False},
                                            {'name': 'Faliro', 'flag_Status': False},
                                            {'name': 'Konstantinopolitika', 'flag_Status': False},
                                            {'name': 'Thermi-Xortiatis', 'flag_Status': False},
                                            {'name': 'Airport', 'flag_Status': False}]

                    flag_HOCL = [{'name': 'OHWCL_Day_1', 'flag_Status': False},
                                 {'name': 'OHWCL_Day_2', 'flag_Status': False},
                                 {'name': 'OHWCL_Day_3', 'flag_Status': False}
                                 ]

                # Open files to store the flags
                flname = 'response_HW_Emerg_FLAGS' + '_' + flag_mode + '.txt'
                outfl = open(flname, 'w')

                while current_date <= end:
                    print("\n ***** Current run ", iteration, " at date/time: ", current_date)

                    if flag_mode == 'Real-Time':
                        # Real Data Heatwave
                        CrisisClassificationHeatwave_RealData_Emerg_v2(iteration, flag_location_status, flag_HOCL, CategThresh)

                    elif flag_mode == 'Fake':
                        # Fake Data Heatwave
                        CrisisClassificationHeatwave_FakeData_Emerg_v2(iteration, flag_location_status, flag_HOCL, CategThresh)

                    # Update flag files
                    outfl.write("\n ------------------------------ \n")
                    msg = " Current run: " + str(iteration) + " at date/time: " + str(current_date)
                    outfl.write(msg)
                    outfl.write("\n")
                    json.dump(flag_location_status, outfl)
                    outfl.write("\n ----*****------  \n")
                    json.dump(flag_HOCL, outfl)

                    current_date += step
                    iteration += 1

                    # sleep for step (3) minutes
                    print("Start sleep")
                    secs = 3 * 60
                    time.sleep(secs)
                    print("Stop sleep")

                # Close files
                outfl.close()

        #-----------------------------------------------------------------------------------------
        elif Topic_Type == 'TOP021_INCIDENT_REPORT':

            if 'uri' in json_msg['body'] and len(json_msg['body']['uri'].split('#')) == 2 and \
                    json_msg['body']['uri'].split('#')[0].endswith('mobileApp'):

                URI = json_msg['body']['uri']
                Position = json_msg['body']['position']
                Sent_Time = json_msg['header']['sentUTC']
                Incident_ID = json_msg['body']['incidentID']

                if 'description' in json_msg['body']:
                    if json_msg['body']['description'].isspace() or len(json_msg['body']['description']) == 0:
                        Incident_Descr = "None"
                    else:
                        Incident_Descr = json_msg['body']['description']
                else:
                    Incident_Descr = "None"

                CrisisClassificationFlood_Emerg_IncRep_v2(current_date, URI, Position, Sent_Time, Incident_ID, Incident_Descr)

            else:
                print("TOP021 not for CRCL")

        else:

            print("Wrong Topic from listener")



from confluent_kafka import Consumer
#from python-confluent-kafka import Consumer
import json
import asyncio
import sqlite3


from CRCL.FloodCRisisCLassification.CRCL_FLOOD_Forecast_v15 import *
from CRCL.FloodCRisisCLassification.CRCL_FLOOD_Sensor_v8 import *
from CRCL.FireCRisisCLassification.CRCL_FIRE_FWI_v11 import *
from CRCL.HeatwaveCRisisCLassification.CRCL_Heatwave_v6 import *

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

        #self.database = 'messages.sqlite'

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

                # print("RECEIVED: " + msg.topic())

                # Add incoming message to requests database
                try:
                    message_text = msg.value().decode('utf-8')
                except:
                    message_text = msg.value()

                self.submit_message_to_crcl(message_text)

                #self.submit_message_to_database(message_text)

            # Sleep for a while
            # asyncio.sleep(0.43)

        # Unsubscribe and close consumer
        self.consumer.unsubscribe()
        self.consumer.close()

    def stop(self):
        self.listening = False

    def submit_message_to_crcl(self, message):

        json_msg = json.loads(message)
        #print(json_msg)

        if json_msg['body']['TaskID'] == "1001":
            CrisisClassificationFlood_PreEmerg()
        elif json_msg['body']['TaskID'] == "1002":
            CrisisClassificationFlood_Emerg()
        elif json_msg['body']['TaskID'] == "2001":
            CrisisClassificationFire_PreEmerg()
        elif json_msg['body']['TaskID'] == "3001":
            CrisisClassificationHeatwave_PreEmerg()



    # def submit_message_to_database(self, message):
    #
    #     try:
    #         con = sqlite3.connect(self.database)
    #
    #         with con:
    #             cur = con.cursor()
    #             cur.execute('INSERT INTO requests (message) VALUES (?)', (message,))
    #
    #         cur.close()
    #
    #     except sqlite3.Error as e:
    #         print("Error %s:" % e.args[0])
    #         return False
    #
    #     # con = sqlite3.connect(self.database)
    #     #
    #     # with con:
    #     #     cur = con.cursor()
    #     #     cur.execute('INSERT INTO requests (message) VALUES (?)', (message,))
    #     #
    #     # cur.close()

from confluent_kafka import Producer
import bus.rest
import json


class BusProducer:
    def __init__(self):

        # Pre-shared credentials
        self.credentials = {
            # "api_key": "BPWTh17zQ2kDvxuvmSoHqZEHEbbu6izktAHKC8aD2EGDVNeO",
            "api_key": "L7QLQDexe9rVuu2n63bnHbrVEAkNLLyzQA6wNPHdUuCKd0Mp",
            "kafka_admin_url": "https://kafka-admin-prod02.messagehub.services.eu-gb.bluemix.net:443",
            "kafka_brokers_sasl": [
                "kafka03-prod02.messagehub.services.eu-gb.bluemix.net:9093",
                "kafka02-prod02.messagehub.services.eu-gb.bluemix.net:9093",
                "kafka04-prod02.messagehub.services.eu-gb.bluemix.net:9093",
                "kafka05-prod02.messagehub.services.eu-gb.bluemix.net:9093",
                "kafka01-prod02.messagehub.services.eu-gb.bluemix.net:9093"
            ]
        }

        # Construct required configuration
        self.configuration = {
            'client.id': 'CRCL_producer',
            'group.id': 'CRCL_producer_group',
            'bootstrap.servers': ','.join(self.credentials['kafka_brokers_sasl']),
            'security.protocol': 'SASL_SSL',
            'ssl.ca.location': '/etc/ssl/certs',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': self.credentials['api_key'][0:16],
            'sasl.password': self.credentials['api_key'][16:48],
            'api.version.request': True
        }

        self.producer = Producer(self.configuration)

    def send(self, topic, message):

        message = json.dumps(message, indent=3)
      #  print("\n INSIDE SEND: ", type( message ) )

        # Check if topic exists and create it if not
        if not self.handle_topic(topic):
            return False

        # Produce and flush message to buss
        try:
            self.producer.produce(topic, message.encode('utf-8'), 'key', -1, self.on_delivery)
            # self.producer.poll(0)
            self.producer.flush()
        except Exception as err:
            print('Sending data failed')
            print(err)
            return False

        return True

    def handle_topic(self, topic_name):

        # Create rest client to handle topics
        try:
            rest_client = bus.rest.MessageHubRest(self.credentials['kafka_admin_url'], self.credentials['api_key'])
        except Exception as e:
            print(e)
            return False

        # List all topics
        try:
            response = rest_client.list_topics()
            topics = json.loads(response.text)
        except Exception as e:
            print(e)
            return False

        # Check if desired topic exists in topic list
        topic_exists = False

        for topic in topics:
            if topic['name'] == topic_name:
                topic_exists = True

        # If topic does not exist
        if not topic_exists:

            # Create topic
            try:
                response = rest_client.create_topic(topic_name, 1, 1)
                print(response.text)
            except Exception as e:
                print(e)
                return False

        return True

    def on_delivery(self, err, msg):
        if err:
            # print('Delivery report: Failed sending message {0}'.format(msg.value()))
            print(err)
            # We could retry sending the message
        else:
            print('Message produced, offset: {0}'.format(msg.offset()))


import os
import random
import time
import logging

from kafka import SimpleProducer, KafkaClient, KeyedProducer
from kafka.common import LeaderNotAvailableError

logging.basicConfig()
logger = logging.getLogger('kafka-app')

# IP:PORT of a Kafka broker. The typical port is 9092.
KAFKA_BROKER_IP_PORT = os.getenv('KAFKA_BROKER', 'cdh-slave0:9092')
print "KAFKA BROKER: " + KAFKA_BROKER_IP_PORT
kafka = KafkaClient(KAFKA_BROKER_IP_PORT)
producer = KeyedProducer(kafka)

# Note that the application is responsible for encoding messages to type str
while True:
    print('Sending...')
    logger.info('Sending...')
    try:
        list=["xxx","yyy"]
        for line in list:

            ran = "_"+str(random.randint(0, 10))
            producer.send_messages("test_sitepvv3","test_sitepvv3"+ran, line)
    except LeaderNotAvailableError:
        logger.warning('Caught a LeaderNotAvailableError. This seems to happen when auto-creating a new topic.')
        print('Caught a LeaderNotAvailableError. This seems to happen when auto-creating a new topic.')
    time.sleep(3)

    # kafka.close()

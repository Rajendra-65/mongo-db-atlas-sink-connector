import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import time
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd


def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))

kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '7T3V6SOL74TXYCOB',
    'sasl.password': '6jtZewmxHWOpWRtPXgVoIPA7DpCDN9GgnSpF3Gcbm2Ni9pyss4/Mvvx7gcqvUNhA'
}

schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-lzvd0.ap-southeast-2.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('7Y5JR4JDXR4DJMVH', 'ueQ3dPMKrqpZP0EoaLmST5pk/KWqlR1bd3px4fsZkbhBRC+if57/3BSU931Coymu')
})

subject_name = 'logistics_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.serializer': key_serializer, 
    'value.serializer': avro_serializer  
})

df = pd.read_csv('logistics_data.csv')
df = df.fillna('null')

for index, row in df.iterrows():
    value = row.to_dict()
    producer.produce(topic='logistics_data', key=str(value['shipmentId']), value=value, on_delivery=delivery_report)
    producer.flush()
    time.sleep(1)

print("All Data successfully published to Kafka")
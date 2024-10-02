import pymongo
import datetime
import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': '7T3V6SOL74TXYCOB',
    'sasl.password': '6jtZewmxHWOpWRtPXgVoIPA7DpCDN9GgnSpF3Gcbm2Ni9pyss4/Mvvx7gcqvUNhA',
    'group.id': 'group1',
    'auto.offset.reset': 'latest'
}

schema_registry_client = SchemaRegistryClient({
    'url': 'https://psrc-lzvd0.ap-southeast-2.aws.confluent.cloud',
    'basic.auth.user.info': '{}:{}'.format('7Y5JR4JDXR4DJMVH', 'ueQ3dPMKrqpZP0EoaLmST5pk/KWqlR1bd3px4fsZkbhBRC+if57/3BSU931Coymu')
})

subject_name = 'logistics_data-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
})

myclient = pymongo.MongoClient("mongodb+srv://Rajendra:65_ODA4@cluster0.c4uzthx.mongodb.net/")
mydb = myclient['logistic_data']
collection = mydb['order_data']

consumer.subscribe(['logistics_data'])

def validate_record(record):
    required_fields = {
        'shipmentId': str,
        'orderId': str,
        'customerName': str,
        'customerAddress': str,
        'customerPhone': str,
        'shippingDate': str,  # Expected format: 'YYYY-MM-DD'
        'deliveryDate': str,  # Expected format: 'YYYY-MM-DD'
        'shipmentStatus': str,
        'carrierName': str,
        'trackingNumber': str,
        'shippingMethod': str,
        'weight': float,
        'dimensions': str,
        'originAddress': str,
        'destinationAddress': str,
        'shipmentCost': float,
        'insurance': str,
        'specialInstructions': str,
        'remarks': str
    }

    for field, field_type in required_fields.items():
        if field not in record:
            print(f"Missing required field: {field}")
            return False
        if not isinstance(record[field], field_type):
            print(f"Incorrect data type for field: {field}. Expected {field_type.__name__}.")
            return False
        if field in ['shippingDate', 'deliveryDate']:
            try:
                datetime.datetime.strptime(record[field], '%Y-%m-%d')
            except ValueError:
                print(f"Incorrect date format for field: {field}. Expected 'YYYY-MM-DD'.")
                return False
    return True

def transform_and_write(record):
    if validate_record(record):
        result = collection.insert_one({
            'shipmentId': record['shipmentId'],
            'orderId': record['orderId'],
            'customerName': record['customerName'],
            'customerAddress': record['customerAddress'],
            'customerPhone': record['customerPhone'],
            'shippingDate': record['shippingDate'],
            'deliveryDate': record['deliveryDate'],
            'shipmentStatus': record['shipmentStatus'],
            'carrierName': record['carrierName'],
            'trackingNumber': record['trackingNumber'],
            'shippingMethod': record['shippingMethod'],
            'weight': record['weight'],
            'dimensions': record['dimensions'],
            'originAddress': record['originAddress'],
            'destinationAddress': record['destinationAddress'],
            'shipmentCost': record['shipmentCost'],
            'insurance': record['insurance'],
            'specialInstructions': record['specialInstructions'],
            'remarks': record['remarks']
        })
        if result.acknowledged:
            print(f'Document inserted with _id: {result.inserted_id}')
        else:
            print('Document insertion failed')
    else:
        print('Data validation failed')
    result = collection.insert_one({
        'shipmentId':record['shipmentId'], 
        'orderId':record['orderId'],
        'customerName':record['customerName'], 
        'customerAddress':record['customerAddress'], 
        'customerPhone':record['customerPhone'],
        'shippingDate':record['shippingDate'], 
        'deliveryDate':record['deliveryDate'], 
        'shipmentStatus':record['shipmentStatus'], 
        'carrierName':record['carrierName'], 
        'trackingNumber':record['trackingNumber'],
        'shippingMethod':record['shippingMethod'], 
        'weight':record['weight'], 
        'dimensions':record['dimensions'], 
        'originAddress':record['originAddress'], 
        'destinationAddress':record['destinationAddress'],
        'shipmentCost':record['shipmentCost'], 
        'insurance':record['insurance'], 
        'specialInstructions':record['specialInstructions'], 
        'remarks':record['remarks']
    })
    if result.acknowledged:
        print(f'Document inserted with _id: {result.inserted_id}')
    else:
        print('Document insertion failed')


try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue
        record = msg.value()
        if record:
            transform_and_write(record)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
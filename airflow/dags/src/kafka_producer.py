from kafka import KafkaProducer
import requests
import json
import avro.schema
import avro.io
import io
import os

def serialize(schema, obj):
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer = avro.io.DatumWriter(schema)
    writer.write(obj, encoder)
    return bytes_writer.getvalue()

def load_init_data():
    schema_file = os.getcwd()+"/dags/src/schema.avsc"
    schema = avro.schema.parse(open(schema_file).read())
    kafka_broker = 'kafka:9092'
    limit = 1000
    offset = 0

    producer = KafkaProducer(bootstrap_servers=[kafka_broker])

    api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit={}&offset={}'.format(limit, offset)
    data_info = requests.get(api_url)
    set_info = json.loads(data_info.text)
    total = set_info['total']

    while offset < total:
        api_url = 'https://publicapi.traffy.in.th/share/teamchadchart/search?limit={}&offset={}'.format(limit, offset)
        data_info = requests.get(api_url)
        set_info = json.loads(data_info.text)
        for data in set_info['results']:
            serialized_data = serialize(schema, data)
            producer.send('data', value=serialized_data)
        offset += 1000
    producer.close()

load_init_data()
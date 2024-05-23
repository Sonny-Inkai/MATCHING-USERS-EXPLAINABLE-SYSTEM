from pymongo import MongoClient
from pymongo.server_api import ServerApi
from confluent_kafka import Consumer
import json
from config import get_config

def preprocess(msg, user_table, rating_table):
    data = json.loads(msg.value().decode('utf-8'))

    print(f"Consumer consume data {data}")
    print('===========================')
    user_infor = {"UID": data['UID'], "age": data['age'], "gender": data['gender'], "job":data['job'], "zip": data['zip']}
    rating = {"UID": data['UID'], "MID": data['MID'], 'rate': data['rate']}
    user_table.insert_one(user_infor)
    rating_table.insert_one(rating)

if __name__ == "__main__":
    config = get_config()
    mongo_uri = config['mongo_uri']
    client = MongoClient(mongo_uri, server_api=ServerApi('1'))
    database_name = "persona"
    collection_table1 = "rating"
    collection_table2 = "user_infor"
    db = client[database_name]
    user_table = db[collection_table2]
    rating_table = db[collection_table1]

    topic_name = "user_input"
    consumer_config = {
        'bootstrap.servers': 'pkc-4nxnd.asia-east2.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'CLUTOCDLYJQYCWQY',
        'sasl.password': 'ay0qrPty30YYX7TXWJMoviigsEVBLVh/+Tkqgf06raIQbwlFHnzAdnma2I0h3VRC',
        'group.id': 'user_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic_name])

    try:
        while True: 
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                preprocess(msg, user_table, rating_table)
    except:
        KeyboardInterrupt()
    finally:
        # Leave group and commit final offsets
        consumer.close()
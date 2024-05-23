from pymongo import MongoClient
from pymongo.server_api import ServerApi
from confluent_kafka import Producer
from flask import Flask, request, jsonify, json

app = Flask(__name__)

# Optional per-message delivery callback (triggered by poll() or flush())
# when a message has been successfully delivered or permanently
# failed delivery (after retries).
def delivery_callback(err, msg):
    if err:
        print('ERROR: Message failed delivery: {}'.format(err))
    else:
        print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
        

@app.route('/submit', methods=['POST'])
def submit_data():
    userid = request.json['UID']
    itemid = request.json['MID']
    rating = request.json['rate']
    age    = request.json['age']
    gender = request.json['gender']
    job    = request.json['job']
    zip    = request.json['zip']
    data = {"UID": userid, "MID": itemid, "rate": rating, "age": age, "gender": gender, "job":job, "zip": zip}
    producer.produce(topic_name, json.dumps(data).encode('utf-8'), callback=delivery_callback)
    # Block until the messages are sent.s
    producer.poll(10000)
    producer.flush()

    return jsonify({'status': 'success', "data": data})

if __name__ == "__main__":

    # setting
    topic_name = "user_input"
    producer_config = {
        'bootstrap.servers': 'pkc-4nxnd.asia-east2.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': 'CLUTOCDLYJQYCWQY',
        'sasl.password': 'ay0qrPty30YYX7TXWJMoviigsEVBLVh/+Tkqgf06raIQbwlFHnzAdnma2I0h3VRC'
    }
    producer = Producer(producer_config)

    # run app
    app.run()



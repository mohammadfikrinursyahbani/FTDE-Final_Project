import json
import polars as pl
from kafka import KafkaConsumer

if __name__ == "__main__":
    from pymongo import MongoClient

    mongo_client = MongoClient("mongodb://root:rootpassword@localhost:27018/")
    consumer = KafkaConsumer("project4-ftde", bootstrap_servers='localhost')
    print("Starting the consumer")

    for msg in consumer:
        print("Received message", msg.value)
        data = json.loads(msg.value)

        db = mongo_client["ftde02"]
        collection = db["final_project"]
        collection.insert_one(data)
        print("Data telah disimpan ke MongoDB")
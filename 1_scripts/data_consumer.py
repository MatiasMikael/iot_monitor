import os
import logging
from confluent_kafka import Consumer, KafkaException
from pymongo import MongoClient
from decouple import config

# Configure logging
LOG_DIR = "5_logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "kafka_consumer.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Kafka configuration
KAFKA_BROKER = config("KAFKA_BROKER")
KAFKA_TOPIC = config("KAFKA_TOPIC")
KAFKA_GROUP_ID = "iot_consumer_group"

# MongoDB configuration
MONGO_URI = config("MONGO_URI")
MONGO_DB = "iot_data"
MONGO_COLLECTION = "sensor_readings"

# Set up Kafka consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': KAFKA_GROUP_ID,
    'auto.offset.reset': 'earliest'
})

# Set up MongoDB client
client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

def consume_data():
    """Consume messages from Kafka and store them in MongoDB."""
    consumer.subscribe([KAFKA_TOPIC])

    try:
        while True:
            msg = consumer.poll(1.0)  # Poll for messages
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    logging.warning(f"End of partition reached: {msg.error()}")
                else:
                    logging.error(f"Kafka error: {msg.error()}")
                continue

            # Process the message
            data = msg.value().decode('utf-8')
            logging.info(f"Received: {data}")

            # Insert into MongoDB
            collection.insert_one({"data": data})
            logging.info("Data stored in MongoDB.")

    except KeyboardInterrupt:
        logging.info("Consumer interrupted by user.")
    finally:
        consumer.close()
        logging.info("Consumer connection closed.")

if __name__ == "__main__":
    logging.info("Starting Kafka Consumer...")
    consume_data()
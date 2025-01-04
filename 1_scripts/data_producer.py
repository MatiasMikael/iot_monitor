import time
import random
import json
import logging
from faker import Faker
from confluent_kafka import Producer
from decouple import config
import os

# Configurations
KAFKA_BROKER = config("KAFKA_BROKER")
KAFKA_TOPIC = config("KAFKA_TOPIC")

# Set up logging
LOG_DIR = "5_logs"
os.makedirs(LOG_DIR, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "kafka_producer.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Faker instance
faker = Faker()

# Kafka producer setup
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def delivery_report(err, msg):
    """Delivery report callback."""
    if err is not None:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_sensor_data():
    """Generate fake sensor data for temperature and humidity."""
    data = {
        "device_id": faker.uuid4(),
        "timestamp": faker.iso8601(),
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "humidity": round(random.uniform(30.0, 70.0), 2)
    }
    return data

def publish_data():
    """Generate and publish data to Kafka topic."""
    while True:
        sensor_data = generate_sensor_data()
        try:
            producer.produce(
                KAFKA_TOPIC, 
                json.dumps(sensor_data).encode('utf-8'), 
                callback=delivery_report
            )
            producer.flush()  # Ensure the message is sent
            logging.info(f"Produced: {sensor_data}")
        except Exception as e:
            logging.error(f"Failed to produce message: {e}")
        time.sleep(2)

if __name__ == "__main__":
    try:
        logging.info("Kafka producer started.")
        publish_data()
    except KeyboardInterrupt:
        logging.info("Kafka producer stopped by user.")
        print("Stopped by user")
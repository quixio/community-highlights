import json
import logging

from faker import Faker
from quixstreams import Application


KAFKA_BROKER_ADDRESS = "localhost:9092"
OUTPUT_TOPIC_NAME = "users-raw"


def main():
    app = Application(
        broker_address=KAFKA_BROKER_ADDRESS,
        auto_create_topics=True
    )

    with app.get_producer() as producer:    
        while True:
            fake = Faker()
            user = {
                "id": fake.uuid4(),
                "name": fake.name(),
                "country_code": fake.country_code()
            }

            user_str = json.dumps(user) 
            logging.info(f"Producing user: {user_str}")
            producer.produce(
                topic=OUTPUT_TOPIC_NAME,
                key=user["id"],
                value=user_str,
            )
            logging.info(f"Produced")
    

if __name__ == "__main__":
    try:
        logging.basicConfig(level="INFO")
        main()
    except KeyboardInterrupt:
        print("Exiting")

import json
import logging

from quixstreams import Application


KAFKA_BROKER_ADDRESS = "localhost:9092"
INPUT_TOPIC_NAME = "users-raw"


app = Application(
    broker_address=KAFKA_BROKER_ADDRESS,
    consumer_group="users-splitter",
    auto_offset_reset="latest",
)

input_topic = app.topic(name=INPUT_TOPIC_NAME, value_deserializer="json")


def main():
    sdf = app.dataframe(topic=input_topic)

    sdf = sdf.apply(split_and_produce)
    
    app.run(sdf)


def split_and_produce(user: dict):
    with app.get_producer() as producer:
        user_str = json.dumps(user)
        
        producer.produce(
            topic=f"users-{user["country_code"]}",
            key=user["id"],
            value=user_str,
        )
    
    logging.info(f"Produced: {user}")


if __name__ == "__main__":
    try:
        logging.basicConfig(level="DEBUG")
        main()
    except KeyboardInterrupt:
        print("Exiting")

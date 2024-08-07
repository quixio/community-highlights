from quixstreams import Application


def main():
    app = Application(
        broker_address="localhost:9092",
        consumer_group="order-explode",
        auto_offset_reset="latest",
    )

    input_topic = app.topic("order-raw", value_deserializer="json")
    output_topic = app.topic("order-items", value_serializer="json")

    sdf = app.dataframe(input_topic)

    # Explode each element in the "items" array into a single record/row
    sdf = sdf.apply(explode_items, expand=True)
    sdf = sdf.to_topic(output_topic)

    app.run(sdf)


def explode_items(order: dict):
    items = [
        {
            "order_id": order["order_id"],
            "timestamp": order["timestamp"],
            **item
        } for item in order["items"]
    ]

    return items


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

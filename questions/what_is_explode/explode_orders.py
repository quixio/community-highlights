from quixstreams import Application


def main():  
    app = Application(
        broker_address="localhost:9092",
        consumer_group="orders-explode",
        auto_offset_reset="latest",
    )

    input_topic = app.topic("orders-raw", value_deserializer="json")
    output_topic = app.topic("order-items", value_serializer="json")

    sdf = app.dataframe(input_topic)

    sdf = (
        # Transform each element in the "items" array transformed into a single item/record/row
        sdf.apply(expand_order_items, stateful=True, expand=True)
            .apply(combine_order_id_with_item, stateful=True)
            .to_topic(output_topic)
    )

    app.run(sdf)


def expand_order_items(row, state):
    order_id = row["order_id"]
    state.set("order_id", order_id)

    timestamp = row["timestamp"]
    state.set("timestamp", timestamp)

    return row["items"]


def combine_order_id_with_item(row, state):
    return { 
        "order_id": state.get("order_id"),
        "timestamp": state.get("timestamp"),
        **row,
    }


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

from quixstreams import Application


def calculate_total_quantity(row, state):
    current_total = state.get("total_quantity", 0)
    current_total += int(row["quantity"])
    state.set("total_quantity", current_total)
    return current_total


def main():
    app = Application(
        broker_address="localhost:9092",
        consumer_group="transform-schema",
        auto_offset_reset="latest"
    )

    input_topic = app.topic("raw", value_deserializer="json")
    output_topic = app.topic("transform-stateful", value_serializer="json")

    sdf = app.dataframe(input_topic)

    sdf["total_quantity"] = sdf.apply(calculate_total_quantity, stateful=True)
    sdf = (
        sdf[["total_quantity"]]
            .print()
            .to_topic(output_topic)
    )

    app.run(sdf)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

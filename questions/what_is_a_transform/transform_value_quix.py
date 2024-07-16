from quixstreams import Application


def transform_value(row):
    match row["clothing_type"]:
        case "T-shirt" | "Sweatshirt":
            row["clothing_type"] = "Top"
        case "Cap" | "Hat":
            row["clothing_type"] = "Accessories"
        case _:
            row["clothing_type"] = "UNKNOWN_TYPE"
    return row


def main():
    app = Application(
        broker_address="localhost:9092",
        consumer_group="transform-value",
        auto_offset_reset="latest"
    )

    input_topic = app.topic("raw", value_deserializer="json")
    output_topic = app.topic("transform-value", value_serializer="json")

    sdf = app.dataframe(input_topic)

    sdf = (
        sdf.apply(transform_value)
            .print()
            .to_topic(output_topic)
    )

    app.run(sdf)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

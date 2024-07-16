from quixstreams import Application


def transform_schema(row):
    match row["clothing_type"]:
        case "T-shirt" | "Sweatshirt":
            row["clothing_category"] = "Top"
        case "Cap" | "Hat":
            row["clothing_category"] = "Accessories"
        case _:
            row["clothing_category"] = "UNKNOWN_TYPE"
    return row


def main():
    app = Application(
        broker_address="localhost:9092",
        consumer_group="transform-schema",
        auto_offset_reset="latest"
    )

    input_topic = app.topic("raw", value_deserializer="json")
    output_topic = app.topic("transform-schema", value_serializer="json")

    sdf = app.dataframe(input_topic)

    sdf = (
        sdf.drop("colour")
            .apply(transform_schema)
            .print()
            .to_topic(output_topic)
    )

    app.run(sdf)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

from quixstreams import Application


def main():
    app = Application(
        broker_address="localhost:9092",
        consumer_group="cdc-explode",
        auto_offset_reset="latest",
    )

    input_topic = app.topic("cdc-raw", value_deserializer="json")
    output_topic = app.topic("cdc-pets", value_serializer="json")

    sdf = app.dataframe(input_topic)

    # Explode each element in the "changes" array into a single record/row
    sdf = sdf.apply(explode_cdc, expand=True)
    sdf = sdf.to_topic(output_topic)

    app.run(sdf)


def explode_cdc(cdc: dict):
    changes = [
        {
            "table_id": cdc["table_id"],
            "table_name": cdc["table_name"],
            **change
        } for change in cdc["changes"]
    ]

    return changes


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

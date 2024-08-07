from quixstreams import Application


def main():
    app = Application(
        broker_address="localhost:9092",
        consumer_group="telemetry-explode",
        auto_offset_reset="latest",
    )

    input_topic = app.topic("telemetry-raw", value_deserializer="json")
    output_topic = app.topic("telemetry-measurements", value_serializer="json")

    sdf = app.dataframe(input_topic)

    # Explode each element in the "data" array into a single record/row
    sdf = sdf.apply(explode_telemetry, expand=True)
    sdf = sdf.to_topic(output_topic)

    app.run(sdf)


def explode_telemetry(telemetry: dict):
    data = [
        {
            "sensor_id": telemetry["sensor_id"],
            **measurement
        } for measurement in telemetry["data"]
    ]

    return data


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

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

    sdf = (
        # Transform each element in the "items" array transformed into a single item/record/row
        sdf.apply(expand_telemetry_measurements, stateful=True, expand=True)
            .apply(combine_sensor_id_with_measurement, stateful=True)
            .to_topic(output_topic)
    )

    app.run(sdf)


def expand_telemetry_measurements(row, state):
    order_id = row["sensor_id"]
    state.set("sensor_id", order_id)

    return row["data"]


def combine_sensor_id_with_measurement(row, state):
    return {
        "sensor_id": state.get("sensor_id"),
        **row,
    }


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

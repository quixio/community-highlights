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

    sdf = (
        # Transform each element in the "items" array transformed into a single item/record/row
        sdf.apply(expand_cdc_changes, stateful=True, expand=True)
            .apply(combine_table_id_with_change, stateful=True)
            .to_topic(output_topic)
    )

    app.run(sdf)


def expand_cdc_changes(row, state):
    table_id = row["table_id"]
    state.set("table_id", table_id)

    table_name = row["table_name"]
    state.set("table_name", table_name)

    return row["changes"]


def combine_table_id_with_change(row, state):
    return {
        "table_id": state.get("table_id"),
        "table_name": state.get("table_name"),
        **row,
    }


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

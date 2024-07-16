import json
import pandas as pd


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
    with open("raw_1.json") as f:
        df = pd.json_normalize(json.load(f))

        df = (
            df.drop(columns="colour")
                .apply(transform_schema, axis=1)
        )

        df.info()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

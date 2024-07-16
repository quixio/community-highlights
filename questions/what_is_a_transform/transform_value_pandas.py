import json
import pandas as pd


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
    with open("raw_1.json") as f:
        df = pd.json_normalize(json.load(f))

        df = df.apply(transform_value, axis=1)

        print(df)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

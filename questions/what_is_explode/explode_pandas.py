import json
import pandas as pd


def main():
    with open("data/order.json") as f:
        df = pd.json_normalize(json.load(f))

        df = df.explode("items")

        # Increase display to the width of values
        pd.options.display.max_colwidth = None

        print(df)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Exiting")

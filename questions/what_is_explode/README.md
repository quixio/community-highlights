# What Is Explode? Top 3 Kafka Python Use Cases

[![YouTube thumbnail](youtube_thumbnail.png?raw=true)](https://www.youtube.com/watch?v=gC7RPqgdyG8)

In data engineering, knowing how to explode data will help you deal with challenges at the intersection where batching systems and streaming systems meet. It helps avoid an event-driven system anti-pattern by ensuring each message contains a single record. Furthermore it makes tasks such as filtering easier and also enables you to pivot data for further analysis and processing.

We’ll go through a batch processing example using Pandas and then we’ll go through 3 top use cases for Python stream processing with Kafka and [Quix Streams](https://github.com/quixio/quix-streams): e-commerce orders, change data capture from databases and telemetry data from sensors. Check out the GitHub repo with the full source code for the use cases and use Docker Compose to try them out on your machine.

📺 Watch on YouTube: https://www.youtube.com/watch?v=gC7RPqgdyG8

## Prerequisites

🐱 kcat: https://github.com/edenhill/kcat \
😻 jq https://github.com/jqlang/jq \
🐋 Docker Compose: https://docs.docker.com/compose

## Run it

```sh
# Set up an environment
cd questions/what_is_explode
python -m venv env
source env/bin/activate
pip install -r requirements.txt

# Run local Redpanda broker with Docker Compose
docker compose up -d

# Run explode for the order use case with Quix Streams
python explode_order.py

# Produce JSON data to the "order-raw" topic with kcat
kcat -b localhost:9092 -t order-raw -P data/order.json
```

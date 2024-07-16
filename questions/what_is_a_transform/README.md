# What Is a Transform?

[![YouTube thumbnail](youtube_thumbnail.png?raw=true)](https://www.youtube.com/watch?v=FC-DrNbe5fA)


When you’re a newcomer to data engineering, data transformations are yet another concept for you to learn. We’ll go through the main concepts using a relatable use case and provide Python examples for how you can apply them using a batch approach with the popular Pandas DataFrame library. Then we’ll take that same code and show you how to modify it to use Streaming DataFrames, bringing it into the world of Kafka and stream processing with [Quix Streams](https://github.com/quixio/quix-streams).

We’ll cover value transformations, schema transformations and stateless/stateful transformations.

📺 Watch on YouTube: https://www.youtube.com/watch?v=FC-DrNbe5fA

## Prerequisites

🐱 kcat: https://github.com/edenhill/kcat \
🐋 Docker Compose: https://docs.docker.com/compose

## Run it

```sh
# Set up an environment
cd questions/what_is_a_transform
python -m venv env
source env/bin/activate
pip install -r requirements.txt

# Run local Redpanda broker with Docker Compose
docker compose up -d

# Run transformation with Quix Streams
python transform_value_quix.py

# Produce JSON data to the "raw" topic with kcat
kcat -P -b localhost:9092 -t raw raw_1.json
```

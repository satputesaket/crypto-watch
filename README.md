# Crypto Watch --- Kafka + Spark Streaming

A local data engineering project that collects cryptocurrency market
data from CoinGecko, publishes it to Apache Kafka, processes it with
Spark Structured Streaming, and stores the flattened data as Parquet
files.

## Architecture

``` text
CoinGecko API
     |
     v
Python Ingestion Script
     |
     v
Kafka topic: crypto-prices
     |
     v
Spark Structured Streaming
     |
     v
Parquet files
     |
     v
app/dataframes/
```

## Project Structure

``` text
crypto-watch/
├── app/
│   ├── checkpoints/
│   ├── dataframes/
│   ├── ingestion/
│   │   └── crypto_market_ingestion.py
│   ├── spark_session/
│   │   └── kafka_streaming.py
│   └── test.py
├── docker-compose.yml
├── requirements.txt
└── .venv/
```

## Prerequisites

Install/configure:

-   Docker Desktop
-   Python 3
-   Git (optional)
-   A CoinGecko API endpoint accessible from the Python application

Check installations:

``` bash
docker --version
docker compose version
python3 --version
```

------------------------------------------------------------------------

# 1. Go to the project

``` bash
cd ~/crypto-watch
```

If your project is somewhere else, use that path instead.

------------------------------------------------------------------------

# 2. Create and activate the Python virtual environment

Create it once:

``` bash
python3 -m venv .venv
```

Activate it:

``` bash
source .venv/bin/activate
```

You should see something similar to:

``` text
(.venv) (base) ...
```

------------------------------------------------------------------------

# 3. Install Python dependencies

Install the requirements:

``` bash
pip install -r requirements.txt
```

The project uses `kafka-python` for sending data from Python to Kafka.

You can verify it with:

``` bash
pip show kafka-python
```

------------------------------------------------------------------------

# 4. Start Kafka and Spark

Start the Docker services:

``` bash
docker compose up -d
```

Check running containers:

``` bash
docker ps
```

You should have containers similar to:

``` text
kafka
spark
```

Check all Compose services:

``` bash
docker compose ps
```

------------------------------------------------------------------------

# 5. If Spark needs to stay running

The Spark container should use:

``` yaml
command: tail -f /dev/null
```

This keeps the container alive so that Spark commands can be run later
using `docker exec`.

If the container is not running:

``` bash
docker compose up -d spark
```

Check:

``` bash
docker ps
```

------------------------------------------------------------------------

# 6. Check Kafka

List Kafka topics:

``` bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

You should eventually see:

``` text
__consumer_offsets
crypto-prices
```

------------------------------------------------------------------------

# 7. Create the Kafka topic

If `crypto-prices` does not exist, create it:

``` bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

Verify:

``` bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

------------------------------------------------------------------------

# 8. Test Kafka manually (optional)

## Start a console consumer

Open Terminal 1:

``` bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --group test-consumer
```

Leave this terminal running.

## Start a console producer

Open Terminal 2:

``` bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --topic crypto-prices \
  --bootstrap-server localhost:9092
```

Type:

``` text
10000
ETH 4500
DOGECOIN 11
```

The consumer should receive the messages.

Stop either console program with:

``` text
Ctrl+C
```

------------------------------------------------------------------------

# 9. Run the Python CoinGecko producer

The Python producer runs on the Mac/host, while Kafka runs inside
Docker.

Therefore the Python producer connects to:

``` text
localhost:9092
```

Run:

``` bash
python app/ingestion/crypto_market_ingestion.py
```

Expected output:

``` text
Streaming filtered crypto data from CoinGecko to Kafka...
Pushed 18 records at 2026-09-05 18:04:10
Pushed 18 records at 2026-09-05 18:04:40
Pushed 18 records at 2026-09-05 18:05:11
```

The script sends a new batch approximately every 30 seconds.

Important:

`Pushed 18 records` means one Kafka message contains an array of 18
crypto records. It does NOT mean 18 separate Kafka messages.

Stop the producer with:

``` text
Ctrl+C
```

You should see:

``` text
Stopped manually.
```

------------------------------------------------------------------------

# 10. Kafka networking --- important

There are two different network locations in this project.

## Python running on the Mac

Use:

``` text
localhost:9092
```

Example:

``` python
KAFKA_BROKER = "localhost:9092"
```

## Spark running inside Docker

Use:

``` text
kafka:29092
```

Example:

``` python
.option("kafka.bootstrap.servers", "kafka:29092")
```

Do NOT use `localhost:9092` from inside the Spark container.

Inside a container, `localhost` means that container itself.

`kafka` is the Docker Compose service name and resolves to the Kafka
container.

------------------------------------------------------------------------

# 11. Spark Structured Streaming configuration

The Spark streaming application is:

``` text
app/spark_session/kafka_streaming.py
```

It should use:

``` python
.option("kafka.bootstrap.servers", "kafka:29092")
```

The output path should be:

``` python
output_path = "file:/app/dataframes"
```

The checkpoint path should be:

``` python
checkpoint_path = "file:/app/checkpoints"
```

This is because the host `app/` directory is mounted into the Spark
container as:

``` text
/app
```

------------------------------------------------------------------------

# 12. Run Spark Structured Streaming

Open another terminal.

Keep the Python producer running in a separate terminal if you want live
data to continue arriving.

Run:

``` bash
docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  /app/spark_session/kafka_streaming.py
```

The important parts are:

``` text
Spark version: 3.5.6
Scala version: 2.12
Kafka connector: spark-sql-kafka-0-10_2.12:3.5.6
```

The Kafka connector version should match the Spark version being used.

The Spark process is a streaming application, so the terminal will
remain running while it waits for/processes Kafka data.

------------------------------------------------------------------------

# 13. What Spark does

The streaming application performs these steps:

``` text
Kafka binary value
       |
       v
CAST(value AS STRING)
       |
       v
Parse JSON
       |
       v
Read the array of crypto records
       |
       v
explode(data)
       |
       v
One row per cryptocurrency
       |
       v
Write Parquet
```

The resulting data contains fields such as:

``` text
timestamp
id
symbol
price
market_cap
total_volume
high_24h
low_24h
last_updated
```

------------------------------------------------------------------------

# 14. Check the Parquet output

After Spark has processed data:

``` bash
ls -R app/dataframes
```

You should see files similar to:

``` text
_spark_metadata/
part-00000-....snappy.parquet
part-00000-....snappy.parquet
part-00000-....snappy.parquet
...
```

The `.parquet` files contain the actual processed data.

The `.crc` files are checksum files and are normal.

`_spark_metadata` is Spark Structured Streaming metadata.

Multiple Parquet files are expected because Spark processes the stream
in micro-batches.

------------------------------------------------------------------------

# 15. Check checkpoints

Run:

``` bash
ls -R app/checkpoints
```

Spark uses the checkpoint directory to store streaming progress/state
information.

Do not normally delete the checkpoint directory while you expect the
streaming query to resume from its previous progress.

------------------------------------------------------------------------

# 16. Read the Parquet data with Spark

Start Spark Shell:

``` bash
docker exec -it spark \
  /opt/spark/bin/spark-shell
```

Then:

``` scala
val df = spark.read.parquet("/app/dataframes")
```

Show the data:

``` scala
df.show(20, false)
```

Show the schema:

``` scala
df.printSchema()
```

Count rows:

``` scala
df.count()
```

Example:

``` text
+-------------------+----------+------+---------+----------+------------+--------+--------+-------------------+
|timestamp          |id        |symbol|price    |market_cap|total_volume |high_24h|low_24h |last_updated       |
+-------------------+----------+------+---------+----------+------------+--------+--------+-------------------+
|...                |bitcoin   |btc   |...      |...       |...         |...     |...     |...                |
|...                |ethereum  |eth   |...      |...       |...         |...     |...     |...                |
+-------------------+----------+------+---------+----------+------------+--------+--------+-------------------+
```

Exit Spark Shell:

``` scala
:quit
```

------------------------------------------------------------------------

# 17. Verify Kafka messages directly

If you want to see what Python is putting into Kafka:

``` bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --group verify-consumer
```

You should see JSON similar to:

``` json
{
  "timestamp": "2026-09-05 18:04:10",
  "data": [
    {
      "id": "bitcoin",
      "symbol": "btc",
      "current_price":  ...
    }
  ]
}
```

------------------------------------------------------------------------

# 18. Check Docker logs

Kafka logs:

``` bash
docker logs kafka
```

Spark logs:

``` bash
docker logs spark
```

Follow logs live:

``` bash
docker logs -f kafka
```

or:

``` bash
docker logs -f spark
```

Stop following logs with:

``` text
Ctrl+C
```

------------------------------------------------------------------------

# 19. Stop the project

Stop the Python producer:

``` text
Ctrl+C
```

Stop the Spark streaming application:

``` text
Ctrl+C
```

Stop Docker services:

``` bash
docker compose down
```

------------------------------------------------------------------------

# 20. Restart the project

Start Docker again:

``` bash
docker compose up -d
```

Check:

``` bash
docker ps
```

Check Kafka topic:

``` bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

If `crypto-prices` is missing, recreate it using the topic creation
command above.

Then start the Python producer:

``` bash
source .venv/bin/activate
python app/ingestion/crypto_market_ingestion.py
```

In another terminal start Spark:

``` bash
docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  /app/spark_session/kafka_streaming.py
```

------------------------------------------------------------------------

# 21. Common problems

## Problem: Kafka producer times out

Error:

``` text
kafka.errors.KafkaTimeoutError:
Failed to update metadata
```

Check Kafka:

``` bash
docker ps
```

Check the topic:

``` bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

For Python on the Mac use:

``` text
localhost:9092
```

For Spark inside Docker use:

``` text
kafka:29092
```

------------------------------------------------------------------------

## Problem: Spark cannot connect to Kafka

Check that `kafka_streaming.py` contains:

``` python
.option("kafka.bootstrap.servers", "kafka:29092")
```

Not:

``` python
.option("kafka.bootstrap.servers", "localhost:9092")
```

------------------------------------------------------------------------

## Problem: Spark cannot find the Python file

The actual path inside the container is:

``` text
/app/spark_session/kafka_streaming.py
```

Run:

``` bash
docker exec -it spark ls -R /app
```

------------------------------------------------------------------------

## Problem: Spark package/cache error

Create the Ivy cache directory:

``` bash
docker exec -it spark mkdir -p /home/spark/.ivy2/cache
```

Then run Spark again:

``` bash
docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  /app/spark_session/kafka_streaming.py
```

------------------------------------------------------------------------

## Problem: No Parquet files appear

Check:

``` bash
ls -R app/dataframes
```

Then check whether Spark is running:

``` bash
docker ps
```

Check Spark logs:

``` bash
docker logs spark
```

Also make sure the Python producer is actually sending data.

------------------------------------------------------------------------

## Problem: Kafka topic disappears after `docker compose down`

If Kafka is configured without persistent storage, removing the Kafka
container can remove its local Kafka data.

Check:

``` bash
docker compose down
docker compose up -d
```

Then:

``` bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

If `crypto-prices` is missing, recreate it.

For a more permanent setup, configure a persistent Kafka data volume in
`docker-compose.yml`.

------------------------------------------------------------------------

# 22. Useful commands --- quick reference

## Docker

``` bash
docker compose up -d
docker compose ps
docker ps
docker compose down
docker logs kafka
docker logs spark
```

## Kafka

``` bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

``` bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic crypto-prices \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

## Python producer

``` bash
source .venv/bin/activate
python app/ingestion/crypto_market_ingestion.py
```

## Spark streaming

``` bash
docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  /app/spark_session/kafka_streaming.py
```

## Output

``` bash
ls -R app/dataframes
```

## Checkpoints

``` bash
ls -R app/checkpoints
```

------------------------------------------------------------------------

# 23. End-to-end startup --- shortest version

Once everything has been configured, the normal workflow is:

### Terminal 1 --- start infrastructure

``` bash
cd ~/crypto-watch
docker compose up -d
docker ps
```

### Terminal 2 --- start Python producer

``` bash
cd ~/crypto-watch
source .venv/bin/activate
python app/ingestion/crypto_market_ingestion.py
```

Expected:

``` text
Streaming filtered crypto data from CoinGecko to Kafka...
Pushed 18 records at ...
Pushed 18 records at ...
Pushed 18 records at ...
```

### Terminal 3 --- start Spark

``` bash
cd ~/crypto-watch

docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  /app/spark_session/kafka_streaming.py
```

### Terminal 4 --- inspect output

``` bash
cd ~/crypto-watch
ls -R app/dataframes
```

Then read the Parquet data with Spark:

``` bash
docker exec -it spark \
  /opt/spark/bin/spark-shell
```

Inside Spark:

``` scala
val df = spark.read.parquet("/app/dataframes")
df.show(20, false)
df.printSchema()
df.count()
```

------------------------------------------------------------------------

# 24. What this project demonstrates

This project demonstrates a basic real-time data engineering pipeline
using:

-   Python
-   REST API ingestion
-   Apache Kafka
-   Kafka producers/consumers
-   Docker
-   Spark Structured Streaming
-   JSON schema parsing
-   Spark `explode`
-   Micro-batch streaming
-   Checkpointing
-   Parquet data storage

The core data flow is:

``` text
API
 ↓
Python
 ↓
Kafka
 ↓
Spark Structured Streaming
 ↓
Transform
 ↓
Parquet
```

This is the current working baseline for the project.

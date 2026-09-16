# Crypto-Watch — Docker, Kafka & Spark Runbook

This document records the commands used to start the local infrastructure,
create the Kafka topic, prepare Spark's Ivy cache, and run the Spark
streaming and analytics jobs.

## 1. Start the Docker Compose services

Start Kafka, PostgreSQL, and Spark in detached mode:

```bash
docker compose up -d
```

Expected result:

```text
✔ Container spark      Started
✔ Container kafka      Started
✔ Container postgres   Started
```

### What this does

`docker compose up -d` starts the services defined in `docker-compose.yml`
in the background.

For this project:

- **Kafka** — message broker for the crypto price stream.
- **PostgreSQL** — persistent relational database for analytics results.
- **Spark** — runs the PySpark streaming and analytics applications.

---

## 2. Stop the Docker Compose services

To stop and remove the containers and Compose network:

```bash
docker compose down
```

Expected result:

```text
✔ Container kafka      Removed
✔ Container postgres   Removed
✔ Container spark      Removed
✔ Network crypto-watch_default Removed
```

### Important

`docker compose down` removes the containers and network, but named volumes
such as the PostgreSQL data volume are retained unless you explicitly use
`docker compose down -v`.

---

## 3. Check Kafka topics

After starting the services, list the topics:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

If the topic has not been created yet, the command may return no topics.

### Why `localhost:9092` works here

The command is executed inside the Kafka container, and Kafka is configured
with an external listener on port `9092`.

For communication between containers, the Kafka service uses its internal
listener, `kafka:29092`.

---

## 4. Create the `crypto-prices` Kafka topic

Create the topic:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create \
  --topic crypto-prices \
  --partitions 1 \
  --replication-factor 1
```

Expected result:

```text
Created topic crypto-prices.
```

### Topic configuration

| Setting | Value | Meaning |
|---|---:|---|
| Topic | `crypto-prices` | Kafka topic receiving crypto price events |
| Partitions | `1` | One partition for this local setup |
| Replication factor | `1` | One copy of the partition |

For a local development environment, one partition and one replica are
appropriate. A production deployment would normally use multiple partitions
and replicas based on throughput and availability requirements.

---

## 5. Verify the Kafka topic

Run:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Expected output:

```text
crypto-prices
```

This confirms that the topic exists.

---

## 6. Prepare Spark Ivy cache permissions

Spark uses Ivy to resolve Maven dependencies specified with `--packages`.

Prepare the Ivy cache:

```bash
docker exec -u 0 -it spark sh -c 'mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark'
```

### Why this is needed

The Spark container runs the application as the `spark` user. Ivy needs to
write dependency metadata and downloaded artifacts under:

```text
/home/spark/.ivy2
```

The command:

1. Runs the shell as root using `-u 0`.
2. Creates `/home/spark/.ivy2/cache` if it does not exist.
3. Changes ownership of `/home/spark` and its contents to `spark:spark`.

---

## 7. Verify Ivy permissions

Run:

```bash
docker exec -it spark sh -c 'ls -ld /home/spark /home/spark/.ivy2 /home/spark/.ivy2/cache'
```

Expected ownership should look similar to:

```text
drwxr-xr-x ... spark spark ... /home/spark
drwxr-xr-x ... spark spark ... /home/spark/.ivy2
drwxr-xr-x ... spark spark ... /home/spark/.ivy2/cache
```

The important part is:

```text
spark spark
```

which indicates that the Spark user owns the directories.

---

## 8. Run the Kafka → Spark Structured Streaming job

Submit the streaming application:

```bash
docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  /workspace/app/spark_session/kafka_streaming.py
```

### What this command does

`docker exec -it spark`
- Executes the command inside the running Spark container.

`/opt/spark/bin/spark-submit`
- Submits a PySpark application to Spark.

`--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6`
- Adds Spark's Kafka Structured Streaming connector.
- The connector version matches Spark `3.5.6`.

`/workspace/app/spark_session/kafka_streaming.py`
- Runs the project's Kafka streaming application.

### Pipeline

The application is intended to follow this flow:

```text
Kafka
  │
  │ crypto-prices
  ▼
Spark Structured Streaming
  │
  │ transformations
  ▼
flattened DataFrame
  │
  ▼
Parquet output
```

Because this is a streaming application, it normally keeps running and waits
for new Kafka records.

`query.awaitTermination()` intentionally blocks the process so that the
stream continues running.

### Stopping the streaming application

Press:

```text
Ctrl+C
```

This requests application shutdown. PySpark/Py4J shutdown messages can
sometimes appear while the Python process is interrupting the JVM connection;
these do not necessarily indicate a problem with the streaming pipeline.

---

## 9. Run the Spark analytics job

Submit the analytics application:

```bash
docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --jars /opt/spark/custom-jars/postgresql-42.7.13.jar \
  /workspace/app/spark_session/analytics.py
```

### What this command does

`--jars /opt/spark/custom-jars/postgresql-42.7.13.jar`
- Adds the PostgreSQL JDBC driver to Spark's classpath.
- This allows Spark to communicate with PostgreSQL through JDBC.

`analytics.py`
- Runs the project's batch analytics application.

The intended flow is:

```text
Parquet / processed data
        │
        ▼
   Spark Analytics
        │
        ▼
    PostgreSQL
```

---

## 10. Typical startup sequence

For a fresh local run, use this order:

### Step 1 — Start infrastructure

```bash
docker compose up -d
```

### Step 2 — Create the Kafka topic if necessary

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create \
  --topic crypto-prices \
  --partitions 1 \
  --replication-factor 1
```

If the topic already exists, Kafka will report that it already exists.

### Step 3 — Prepare Ivy permissions

```bash
docker exec -u 0 -it spark sh -c 'mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark'
```

### Step 4 — Start the streaming job

```bash
docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  /workspace/app/spark_session/kafka_streaming.py
```

Keep this process running while you want Kafka records to be consumed.

### Step 5 — Run analytics

In another terminal:

```bash
docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --jars /opt/spark/custom-jars/postgresql-42.7.13.jar \
  /workspace/app/spark_session/analytics.py
```

---

## 11. Important Docker/Kafka distinction

There are two Kafka addresses used in this setup:

```text
localhost:9092
```

and

```text
kafka:29092
```

### From the host machine

Use:

```text
localhost:9092
```

Example:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### From another Docker container

Use:

```text
kafka:29092
```

Example:

```text
Spark → Kafka
       kafka:29092
```

The hostname `kafka` is resolved through the Docker Compose network.

---

## 12. Shell multiline command warning

When using `\` for a multiline shell command, the backslash must be the
**last character on the line**.

Correct:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \
  --create
```

Do not put spaces after the backslash:

```bash
# Incorrect
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \   
  --bootstrap-server kafka:29092
```

If the shell interprets the backslash incorrectly, arguments such as
`--create` can be interpreted as separate shell commands.

For commands that are easy to mistype, using a single line is also safe:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --create --topic crypto-prices --partitions 1 --replication-factor 1
```

---

## 13. Quick verification commands

### Check running containers

```bash
docker compose ps
```

### Check Kafka topic

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### Check Spark workspace

```bash
docker exec -it spark ls -la /workspace
```

### Check the streaming output directory

```bash
docker exec -it spark find /workspace/dataframes -maxdepth 2 -type f
```

### Check checkpoint directory

```bash
docker exec -it spark find /workspace/checkpoints -maxdepth 2 -type f
```

---

## 14. Current architecture

```text
                 ┌──────────────────────┐
                 │   Crypto API / Data  │
                 └──────────┬───────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │     Kafka     │
                    │ crypto-prices │
                    └───────┬───────┘
                            │
                            ▼
                ┌───────────────────────┐
                │ Spark Structured      │
                │ Streaming             │
                │ kafka_streaming.py    │
                └───────────┬───────────┘
                            │
                            ▼
                     ┌─────────────┐
                     │   Parquet   │
                     │  dataframes │
                     └──────┬──────┘
                            │
                            ▼
                  ┌──────────────────┐
                  │ Spark Analytics  │
                  │   analytics.py   │
                  └────────┬─────────┘
                           │ JDBC
                           ▼
                    ┌─────────────┐
                    │ PostgreSQL  │
                    │ crypto_metrics│
                    └─────────────┘
```

This runbook reflects the commands used in the current local
`crypto-watch` Docker/Spark setup.

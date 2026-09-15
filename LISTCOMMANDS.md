## INGESTION

###prev
```
python app/ingestion/crypto_market_ingestion.py

```
### latest
```
(.venv) (base) saket@ADITIs-MacBook-Pro crypto-watch % python -m app.ingestion.crypto_market_ingestion
```


## KAFKA STREAMING script 

#### check if topics exist

```
crypto-watch % docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

```

#### if not create
```
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:29092 \   
  --create \
  --topic crypto-prices \
  --partitions 1 \
  --replication-factor 1

```


#### because the spark is in docker the .ivy2/cache folder might be missing
```
docker exec -u 0 -it spark sh -c 'mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark'

```

```
docker exec -it spark sh -c 'ls -ld /home/spark /home/spark/.ivy2 /home/spark/.ivy2/cache' 
```


#### actual script
```
docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  /app/spark_session/kafka_streaming.py

```


## analytics.py

```
saket@ADITIs-MacBook-Pro crypto-watch % docker exec -it spark \
  /opt/spark/bin/spark-submit \
  --jars /opt/spark/custom-jars/postgresql-42.7.13.jar \
  /app/spark_session/analytics.py

```
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, ArrayType
from app.configuration.config import config

import logging
from app.configuration.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# ---------------------------------------------------------
# Schemas
# ---------------------------------------------------------

def get_coin_schema() -> StructType:
    """Return schema for an individual cryptocurrency."""

    return StructType([
        StructField("id", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("current_price", DoubleType(), True),
        StructField("market_cap", LongType(), True),
        StructField("total_volume", DoubleType(), True),
        StructField("high_24h", DoubleType(), True),
        StructField("low_24h", DoubleType(), True),
        StructField("last_updated", StringType(), True),
    ])


def get_message_schema() -> StructType:
    """Return schema for the Kafka message."""

    return StructType([
        StructField("timestamp", StringType(), True),
        StructField(
            "data",
            ArrayType(get_coin_schema()),
            True,
        ),
    ])


# ---------------------------------------------------------
# Spark
# ---------------------------------------------------------

def create_spark_session() -> SparkSession:
    """Create and configure SparkSession."""

    spark = (
        SparkSession.builder
        .appName(config["spark"]["ingestion_app_name"])
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ---------------------------------------------------------
# Kafka
# ---------------------------------------------------------

def read_from_kafka(spark: SparkSession) -> DataFrame:
    """Create streaming DataFrame from Kafka."""

    topic = config["kafka"]["topic"]
    bootstrap_servers = config["kafka"]["bootstrap_servers_internal"]

    logger.info(
        "Reading from Kafka topic: %s",
        topic,
    )

    return (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            bootstrap_servers,
        )
        .option(
            "subscribe",
            topic,
        )
        .option(
            "startingOffsets",
            "earliest",
        )
        .option(
            "failOnDataLoss",
            "false",
        )
        .load()
    )


# ---------------------------------------------------------
# Transformation
# ---------------------------------------------------------

def parse_kafka_messages(
    raw_df: DataFrame,
) -> DataFrame:
    """Convert Kafka binary values into parsed JSON."""

    schema = get_message_schema()

    json_df = raw_df.selectExpr(
        "CAST(value AS STRING) AS json"
    )

    return json_df.select(
        from_json(
            col("json"),
            schema,
        ).alias("parsed")
    )


def flatten_coin_data(
    parsed_df: DataFrame,
) -> DataFrame:
    """Flatten nested cryptocurrency data."""

    return (
        parsed_df
        .select(
            col("parsed.timestamp").alias("timestamp"),
            explode(
                col("parsed.data")
            ).alias("coin"),
        )
        .select(
            "timestamp",
            col("coin.id").alias("id"),
            col("coin.symbol").alias("symbol"),
            col("coin.current_price").alias("price"),
            col("coin.market_cap"),
            col("coin.total_volume"),
            col("coin.high_24h"),
            col("coin.low_24h"),
            col("coin.last_updated"),
        )
    )


# ---------------------------------------------------------
# Parquet
# ---------------------------------------------------------

def write_to_parquet(
    dataframe: DataFrame,
) -> object:
    """Write streaming DataFrame to Parquet."""

    output_path = config["spark"]["output_path"]
    checkpoint_path = config["spark"]["checkpoint_path"]
    processing_time = config["spark"]["processing_time"]

    logger.info(
        "Writing streaming data to: %s",
        output_path,
    )

    return (
        dataframe.writeStream
        .format("parquet")
        .outputMode("append")
        .trigger(
            processingTime=processing_time
        )
        .option(
            "path",
            output_path,
        )
        .option(
            "checkpointLocation",
            checkpoint_path,
        )
        .start()
    )


# ---------------------------------------------------------
# Application
# ---------------------------------------------------------

def run_streaming_job() -> None:
    """Run Kafka → Spark → Parquet streaming pipeline."""

    spark = create_spark_session()

    logger.info("Starting Kafka streaming job")

    try:
        raw_df = read_from_kafka(spark)

        parsed_df = parse_kafka_messages(
            raw_df
        )

        flattened_df = flatten_coin_data(
            parsed_df
        )

        query = write_to_parquet(
            flattened_df
        )

        query.awaitTermination()

    except Exception:
        logger.exception(
            "Kafka streaming job failed"
        )
        raise

    finally:
        logger.info(
            "Kafka streaming job stopped"
        )

        spark.stop()


# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    run_streaming_job()
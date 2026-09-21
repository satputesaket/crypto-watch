import time
from pyspark.sql import DataFrame,SparkSession
from pyspark.sql.functions import col, to_timestamp, avg, stddev, lag, lit,current_timestamp,expr, round, when, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
from app.configuration.config import config
import os

import logging

from app.configuration.logging_config import setup_logging

setup_logging()

logger = logging.getLogger(__name__)

# postgres_password = os.getenv("POSTGRES_PASSWORD")

# if not postgres_password:
#     raise RuntimeError("POSTGRES_PASSWORD environment variable is not set")

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

ANALYTICS_INTERVAL = 360
LOOKBACK_MINUTES = 7

PARQUET_PATH = config["spark"]["output_path"]

POSTGRES_CONFIG = config["postgres"]

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_CONFIG['host']}:"
    f"{POSTGRES_CONFIG['port']}/"
    f"{POSTGRES_CONFIG['database']}"
)

MAIN_TABLE = config["analytics"]["main_table"]
GAINERS_TABLE = config["analytics"]["gainers_table"]
LOSERS_TABLE = config["analytics"]["losers_table"]


# ---------------------------------------------------------
# PostgreSQL
# ---------------------------------------------------------

def get_postgres_properties() -> dict:
    """Build PostgreSQL JDBC connection properties."""

    postgres_password = os.getenv("POSTGRES_PASSWORD")

    if not postgres_password:
        raise RuntimeError(
            "POSTGRES_PASSWORD environment variable is not set"
        )

    return {
        "user": POSTGRES_CONFIG["user"],
        "password": postgres_password,
        "driver": POSTGRES_CONFIG["driver"],
    }


# ---------------------------------------------------------
# Spark
# ---------------------------------------------------------

def create_spark_session() -> SparkSession:
    """Create and configure SparkSession."""

    spark = (
        SparkSession.builder
        .appName(config["spark"]["analytics_app_name"])
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark


# ---------------------------------------------------------
# Data loading
# ---------------------------------------------------------

def load_parquet_data(
    spark: SparkSession,
) -> DataFrame:
    """Load cryptocurrency data from Parquet."""

    logger.info(
        "Reading analytics input from: %s",
        PARQUET_PATH,
    )

    return (
        spark.read
        .parquet(PARQUET_PATH)
        .drop(
            "market_cap",
            "total_volume",
            "high_24h",
            "low_24h",
            "last_updated",
        )
    )


# ---------------------------------------------------------
# Data preparation
# ---------------------------------------------------------

def prepare_data(
    df: DataFrame,
) -> DataFrame:
    """Cast columns and retain recent records."""

    df = (
        df
        .withColumn(
            "timestamp",
            to_timestamp("timestamp"),
        )
        .withColumn(
            "price",
            col("price").cast(DoubleType()),
        )
    )

    return df.filter(
        col("timestamp")
        >= current_timestamp() - expr(
            f"INTERVAL {LOOKBACK_MINUTES} MINUTES"
        )
    )


# ---------------------------------------------------------
# Price changes
# ---------------------------------------------------------

def calculate_price_changes(
    df: DataFrame,
) -> DataFrame:
    """Calculate 1-minute and 5-minute price changes."""

    coin_window = (
        Window
        .partitionBy("id")
        .orderBy("timestamp")
    )

    df = (
        df
        .withColumn(
            "price_1min_ago",
            lag("price", 2).over(coin_window),
        )
        .withColumn(
            "price_5min_ago",
            lag("price", 10).over(coin_window),
        )
        .withColumn(
            "change_1min",
            when(
                col("price_1min_ago").isNull(),
                None,
            ).otherwise(
                round(
                    (
                        (col("price") - col("price_1min_ago"))
                        / col("price_1min_ago")
                    )
                    * 100,
                    2,
                )
            ),
        )
        .withColumn(
            "change_5min",
            when(
                col("price_5min_ago").isNull(),
                None,
            ).otherwise(
                round(
                    (
                        (col("price") - col("price_5min_ago"))
                        / col("price_5min_ago")
                    )
                    * 100,
                    2,
                )
            ),
        )
        .drop(
            "price_1min_ago",
            "price_5min_ago",
        )
    )

    return df


# ---------------------------------------------------------
# Technical metrics
# ---------------------------------------------------------

def calculate_metrics(
    df: DataFrame,
) -> DataFrame:
    """Calculate SMA, EMA and volatility."""

    coin_window = (
        Window
        .partitionBy("id")
        .orderBy("timestamp")
    )

    return (
        df
        .withColumn(
            "SMA",
            avg("price").over(
                coin_window.rowsBetween(-4, 0)
            ),
        )
        .withColumn(
            "EMA",
            avg("price").over(
                coin_window.rowsBetween(-2, 0)
            ),
        )
        .withColumn(
            "volatility",
            stddev("price").over(
                coin_window.rowsBetween(-4, 0)
            ),
        )
    )


# ---------------------------------------------------------
# Latest records
# ---------------------------------------------------------

def get_latest_per_coin(
    df: DataFrame,
) -> DataFrame:
    """Get the latest record for every cryptocurrency."""

    latest_window = (
        Window
        .partitionBy("id")
        .orderBy(
            col("timestamp").desc()
        )
    )

    return (
        df
        .withColumn(
            "row_num",
            row_number().over(latest_window),
        )
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


# ---------------------------------------------------------
# Gainers and losers
# ---------------------------------------------------------

def calculate_gainers(
    latest_per_coin: DataFrame,
) -> DataFrame:
    """Return top 5 gainers based on 5-minute price change."""

    rank_window = Window.orderBy(
        col("change_5min").desc()
    )

    return (
        latest_per_coin
        .filter(
            col("change_5min").isNotNull()
        )
        .withColumn(
            "rank",
            row_number().over(rank_window),
        )
        .filter(col("rank") <= 5)
        .select(
            "rank",
            "id",
            "symbol",
        )
    )


def calculate_losers(
    latest_per_coin: DataFrame,
) -> DataFrame:
    """Return top 5 losers based on 5-minute price change."""

    rank_window = Window.orderBy(
        col("change_5min").asc()
    )

    return (
        latest_per_coin
        .filter(
            col("change_5min").isNotNull()
        )
        .withColumn(
            "rank",
            row_number().over(rank_window),
        )
        .filter(col("rank") <= 5)
        .select(
            "rank",
            "id",
            "symbol",
        )
    )


# ---------------------------------------------------------
# Output
# ---------------------------------------------------------

def log_results(
    latest_data: DataFrame,
    gainers: DataFrame,
    losers: DataFrame,
) -> None:
    """Log analytics results."""

    logger.info("=== All Metrics (Latest Snapshot) ===")

    latest_data.select(
        "timestamp",
        "id",
        "symbol",
        "price",
        "change_1min",
        "change_5min",
        "SMA",
        "EMA",
        "volatility",
    ).orderBy("timestamp").show(
        truncate=False
    )

    logger.info("=== Top 5 Gainers ===")
    gainers.show(truncate=False)

    logger.info("=== Top 5 Losers ===")
    losers.show(truncate=False)


# ---------------------------------------------------------
# PostgreSQL output
# ---------------------------------------------------------

def write_to_postgres(
    dataframe: DataFrame,
    table: str,
    properties: dict,
    mode: str = "append",
) -> None:
    """Write a DataFrame to PostgreSQL."""

    logger.info(
        "Writing data to PostgreSQL table: %s",
        table,
    )

    dataframe.write.jdbc(
        url=JDBC_URL,
        table=table,
        mode=mode,
        properties=properties,
    )


# ---------------------------------------------------------
# Analytics cycle
# ---------------------------------------------------------

def run_analytics_cycle(
    spark: SparkSession,
) -> None:
    """Execute one analytics cycle."""

    logger.info("Starting analytics cycle")

    df = load_parquet_data(spark)

    latest_data = prepare_data(df)

    row_count = latest_data.count()

    if row_count == 0:
        logger.info(
            "No data found in the last %d minutes. "
            "Skipping this cycle.",
            LOOKBACK_MINUTES,
        )
        return

    logger.info(
        "Processing %d records",
        row_count,
    )

    latest_data = calculate_price_changes(
        latest_data
    )

    latest_data = calculate_metrics(
        latest_data
    )

    latest_per_coin = get_latest_per_coin(
        latest_data
    )

    gainers = calculate_gainers(
        latest_per_coin
    )

    losers = calculate_losers(
        latest_per_coin
    )

    log_results(
        latest_data,
        gainers,
        losers,
    )

    postgres_properties = get_postgres_properties()

    write_to_postgres(
        latest_data,
        MAIN_TABLE,
        postgres_properties,
        mode="append",
    )

    write_to_postgres(
        gainers,
        GAINERS_TABLE,
        postgres_properties,
        mode="overwrite",
    )

    write_to_postgres(
        losers,
        LOSERS_TABLE,
        postgres_properties,
        mode="overwrite",
    )

    logger.info(
        "Analytics results successfully written to PostgreSQL"
    )


# ---------------------------------------------------------
# Application
# ---------------------------------------------------------

def run_analytics_job() -> None:
    """Continuously execute analytics cycles."""

    spark = create_spark_session()

    logger.info("Starting Spark analytics job")

    try:
        while True:

            try:
                run_analytics_cycle(spark)

            except Exception:
                logger.exception(
                    "Error during analytics cycle"
                )

            logger.info(
                "Waiting %d seconds before next cycle",
                ANALYTICS_INTERVAL,
            )

            time.sleep(ANALYTICS_INTERVAL)

    except KeyboardInterrupt:
        logger.info(
            "Analytics job stopped manually"
        )

    finally:
        logger.info(
            "Stopping Spark session"
        )
        spark.stop()


# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------

if __name__ == "__main__":
    run_analytics_job()
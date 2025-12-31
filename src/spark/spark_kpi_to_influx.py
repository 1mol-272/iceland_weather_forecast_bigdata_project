from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, from_unixtime,
    window, avg, max as fmax, sum as fsum, when, lit
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import os
import urllib.request
import urllib.parse


SCHEMA = StructType([
    StructField("location_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("forecast_time", LongType(), True),
    StructField("ingest_time", LongType(), True),
    StructField("wind_speed_10m", DoubleType(), True),
    StructField("wind_gusts_10m", DoubleType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("precipitation", DoubleType(), True),
    StructField("pressure_msl", DoubleType(), True),
])


def influx_write_lines(url: str, org: str, bucket: str, token: str, lines: str) -> None:
    write_url = f"{url.rstrip('/')}/api/v2/write?org={urllib.parse.quote(org)}&bucket={urllib.parse.quote(bucket)}&precision=s"
    req = urllib.request.Request(write_url, data=lines.encode("utf-8"), method="POST")
    req.add_header("Authorization", f"Token {token}")
    req.add_header("Content-Type", "text/plain; charset=utf-8")
    with urllib.request.urlopen(req, timeout=15) as resp:
        resp.read()


def rows_to_line_protocol(rows, measurement: str, window_label: str, ts_col: str = "window_end_s") -> str:
    # Influx line protocol:
    # measurement,tag1=v1,tag2=v2 field1=...,field2=... timestamp
    # timestamp is in seconds 
    lines = []
    for r in rows:
        loc = r["location_id"]
        # tags
        tags = f"location_id={loc},window={window_label}"

        # fields (ensure numeric fields not None)
        fields = []
        def add_field(name, val):
            if val is None:
                return
            if isinstance(val, bool):
                fields.append(f'{name}={str(val).lower()}')
            elif isinstance(val, int):
                fields.append(f"{name}={val}i")
            else:
                fields.append(f"{name}={float(val)}")

        add_field("avg_wind_speed_10m", r["avg_wind_speed_10m"])
        add_field("max_wind_speed_10m", r["max_wind_speed_10m"])
        add_field("max_wind_gusts_10m", r["max_wind_gusts_10m"])
        add_field("extreme_flag", r["extreme_flag"])
        add_field("extreme_duration_hours", r["extreme_duration_hours"])
        add_field("severity_score", r["severity_score"])

        if not fields:
            continue

        ts = r[ts_col]
        if ts is None:
            continue

        lines.append(f"{measurement},{tags} " + ",".join(fields) + f" {int(ts)}")
    return "\n".join(lines)


def foreach_batch_writer(influx_url, org, bucket, token, measurement, window_label):
    def _write(batch_df, batch_id: int):
        if batch_df.rdd.isEmpty():
            return

        # Collect only KPI rows (should be small: locations x windows)
        rows = batch_df.select(
            "location_id",
            "avg_wind_speed_10m",
            "max_wind_speed_10m",
            "max_wind_gusts_10m",
            "extreme_flag",
            "extreme_duration_hours",
            "severity_score",
            "window_end_s",
        ).collect()

        lines = rows_to_line_protocol([r.asDict() for r in rows], measurement, window_label)
        if not lines.strip():
            return

        influx_write_lines(influx_url, org, bucket, token, lines)

    return _write


def main():
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP")
    kafka_topic = os.environ.get("KAFKA_TOPIC", "weather_iceland_raw")

    influx_url = os.environ.get("INFLUX_URL", "http://10.0.0.36:8086")
    influx_org = os.environ.get("INFLUX_ORG", "weather-lab")
    influx_bucket = os.environ.get("INFLUX_BUCKET", "weather")
    influx_token = os.environ.get("INFLUX_TOKEN", "")

    if not kafka_bootstrap:
        raise RuntimeError("Missing KAFKA_BOOTSTRAP env")
    if not influx_token:
        raise RuntimeError("Missing INFLUX_TOKEN env")

    extreme_gust_threshold = float(os.environ.get("EXTREME_GUST_THRESHOLD", "25.0"))

    spark = (
        SparkSession.builder
        .appName(os.environ.get("SPARK_KPI_APP_NAME", "WeatherKPIToInflux"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), SCHEMA).alias("j"))
        .select("j.*")
        .withColumn("event_time", to_timestamp(from_unixtime(col("forecast_time"))))
        .withWatermark("event_time", "1 hour")
    )

    # define extreme condition
    is_extreme = (col("wind_gusts_10m") >= lit(extreme_gust_threshold))

    # 3h window KPIs (slide 10m)
    w3 = (
        parsed.groupBy(window(col("event_time"), "3 hours", "10 minutes"), col("location_id"))
        .agg(
            avg(col("wind_speed_10m")).alias("avg_wind_speed_10m"),
            fmax(col("wind_speed_10m")).alias("max_wind_speed_10m"),
            fmax(col("wind_gusts_10m")).alias("max_wind_gusts_10m"),
            fmax(when(is_extreme, 1).otherwise(0)).alias("extreme_flag"),
            fsum(when(is_extreme, 1).otherwise(0)).alias("extreme_duration_hours"),
        )
        .withColumn("severity_score",
                    col("extreme_duration_hours") * (col("max_wind_gusts_10m") / lit(extreme_gust_threshold)))
        .withColumn("window_end_s", (col("window").getField("end").cast("long")))
    )

    # 6h window KPIs (slide 10m)
    w6 = (
        parsed.groupBy(window(col("event_time"), "6 hours", "10 minutes"), col("location_id"))
        .agg(
            avg(col("wind_speed_10m")).alias("avg_wind_speed_10m"),
            fmax(col("wind_speed_10m")).alias("max_wind_speed_10m"),
            fmax(col("wind_gusts_10m")).alias("max_wind_gusts_10m"),
            fmax(when(is_extreme, 1).otherwise(0)).alias("extreme_flag"),
            fsum(when(is_extreme, 1).otherwise(0)).alias("extreme_duration_hours"),
        )
        .withColumn("severity_score",
                    col("extreme_duration_hours") * (col("max_wind_gusts_10m") / lit(extreme_gust_threshold)))
        .withColumn("window_end_s", (col("window").getField("end").cast("long")))
    )

    # Write to Influx as two measurements 
    q3 = (
        w3.writeStream
        .outputMode("update")
        .foreachBatch(foreach_batch_writer(influx_url, influx_org, influx_bucket, influx_token,
                                           measurement="wind_kpi", window_label="3h"))
        .option("checkpointLocation", "file:///tmp/checkpoints/weather_kpi_3h")
        .start()
    )

    q6 = (
        w6.writeStream
        .outputMode("update")
        .foreachBatch(foreach_batch_writer(influx_url, influx_org, influx_bucket, influx_token,
                                           measurement="wind_kpi", window_label="6h"))
        .option("checkpointLocation", "file:///tmp/checkpoints/weather_kpi_6h")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()

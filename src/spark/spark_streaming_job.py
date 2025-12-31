from pyspark.sql import SparkSession

def main():
    
    spark = (
        SparkSession.builder
        .appName("KafkaWeatherToHDFS")
        .getOrCreate()
    )

    
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "master:9092")   
        .option("subscribe", "weather_iceland_raw")         
        .option("startingOffsets", "latest")                
        .load()
    )

    lines = df.selectExpr("CAST(value AS STRING) AS value")

    query = (
        lines.writeStream
        .format("parquet")
        .option("path", "/data/weather/raw")    
        .option("checkpointLocation", "/checkpoints/weather_raw")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()


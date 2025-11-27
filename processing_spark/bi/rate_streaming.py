from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, lit, broadcast, max as spark_max
from pyspark.sql.types import StructType, StructField, LongType, IntegerType, TimestampType, StringType
from pyspark.sql.streaming import StreamingQueryListener
import datetime

def main():
    # --- Configuration ---
    CSV_PATH = "/home/siddharth/StreamingDataSystems/endsem_project/generation/bi_signal.csv"
    CHECKPOINT_PATH = "/tmp/spark_checkpoint/pyspark_window_count"
    ROWS_PER_SECOND = 10000
    
    # FIX: Added spark.driver.host to ensure Spark advertises the same IP it binds to.
    spark = SparkSession.builder \
        .appName("EventWindowCountPySpark") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # --- 1. Load Static Data ---
    # We define schema explicitly to avoid Integer/Long casting errors
    schema = StructType([
        StructField("unique_id", LongType(), True),
        StructField("event_type", IntegerType(), True),
        StructField("caller", StringType(), True),
        StructField("callee", StringType(), True),
        StructField("timestamp", LongType(), True),
        StructField("disposition", StringType(), True),
        StructField("imei", StringType(), True),
        StructField("id", LongType(), True)
    ])

    static_df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(CSV_PATH) \
        .withColumnRenamed("timestamp", "event_timestamp_ms") \
        .withColumnRenamed("id", "join_id")
    
    static_df.show(50, truncate=False)

    # Cache for performance since we join against this repeatedly
    static_df.cache()
    row_count = static_df.count()

    # --- 2. Rate Stream (Simulate Data Ingestion) ---
    rate_stream = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", ROWS_PER_SECOND) \
        .load()
    
    # --- 3. Join to create infinite stream of events and define event_time ---
    # The 'timestamp' column from the rate_stream is the synthetic, advancing arrival time.
    stream_with_data = rate_stream \
        .withColumn("join_id", col("value") % row_count) \
        .join(broadcast(static_df), "join_id") \
        .withColumnRenamed("timestamp", "rateTimestamp")
        
    stream_with_time = stream_with_data.withColumn(
        "event_time",
        (col("event_timestamp_ms") / 1000).cast(TimestampType())
    )
    
    # --- 4. Debug Stream (Temporary) ---
    # This query runs in parallel to the main query to inspect the raw incoming data.
    print("--- Starting Debug Query (look for 'Debug Raw Data') ---")

    # --- 5. Watermark Listener (Defined by User) ---
    class WatermarkListener(StreamingQueryListener):
        def onQueryStarted(self, event):
            pass

        def onQueryTerminated(self, event):
            pass

        def onQueryProgress(self, event):
            wm = event.progress.eventTime.get("watermark")

            if wm:
                # Note: 'Z' for Zulu time/UTC is often read as '+0000' for proper parsing
                dt = datetime.datetime.strptime(wm.replace('Z', '+0000'), "%Y-%m-%dT%H:%M:%S.%f%z")
                current_wm_epoch = int(dt.timestamp())
                print(
                    f"[WM] batch={event.progress.batchId} wm={current_wm_epoch}s "
                    f"processing_time={datetime.datetime.now().isoformat()} "
                    f"rows={event.progress.numInputRows}"
                )

    spark.streams.addListener(WatermarkListener())

    # --- 6. Windowed Aggregation Logic ---
    windowed_counts = stream_with_time \
        .filter(col("event_type") == 0) \
        .withWatermark("event_time", "1 second") \
        .groupBy(
            window(col("event_time"), "30 seconds")
        ) \
        .agg(
            count("*").alias("start_event_count"),
            spark_max("rateTimestamp").alias("max_rate_ts")) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("start_event_count"),
            col("max_rate_ts")
        ) 
        # Removed .orderBy("window_start") because it is not supported in 'update' output mode

    # --- 7. Output to Console (Main Query) ---
    # OutputMode "Update" allows us to see the counts update as new data arrives for the window.
    query = windowed_counts.writeStream \
        .outputMode("append") \
        .queryName("Main_Window_Count") \
        .format("console") \
        .option("truncate", "false") \
        .start()
    
    memory_query = windowed_counts.writeStream \
        .queryName("InMemoryWindow") \
        .format("memory") \
        .outputMode("append") \
        .start()

    # Await termination on the main query.
    query.awaitTermination()
    
    # Stop the debug query when main query terminates

if __name__ == "__main__":
    main()
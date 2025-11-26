import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.streaming import StreamingQueryListener

TOTAL_WINDOW_SIZE = 40
WATERMARK = 10
TRIGGER_INTERVAL_S = 1 
IDLE_GAP_THRESHOLD_MS = 900000 # 15 minutes

# --- Spark Session and Data Loading ---
spark = (
    SparkSession.builder
        .appName("StreamingIdleGap_Accumulator")
        .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

csv_path = "/home/siddharth/StreamingDataSystems/endsem_project/generation/mono_signal.csv"

csv_df = (
    spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(csv_path)
)

print("=== Static CSV Data ===")
csv_df.show(1)

row_count = csv_df.count()

if "id" not in csv_df.columns:
    raise Exception("CSV file must contain an 'id' column for the join.")
if "start_ts" not in csv_df.columns or "end_ts" not in csv_df.columns:
    raise Exception("CSV file must contain 'start_ts' and 'end_ts' columns (in milliseconds).")


rate_df = (
    spark.readStream
        .format("rate")
        .option("rowsPerSecond", 1000) # Reduced rate for better stability during accumulation
        .option("rampUpTime", 0)
        .load()
)

stream_with_id = rate_df.withColumn("id", (col("value") % row_count) + 1)

GLOBAL_DATA_ACCUMULATOR = []

initial_schema = csv_df.withColumn("timestamp", col("id")).withColumn("value", col("id")).select("timestamp", "value", "id", *csv_df.columns).schema

spark.createDataFrame(GLOBAL_DATA_ACCUMULATOR, schema=initial_schema).createOrReplaceTempView("static_data_store")

def sql_query(timestamp):
    """Executes the complex self-join query against the accumulated data."""
    
    print(f"\n[ANALYSIS] Triggering Batch SQL for Watermark: {timestamp}s")
    
    query = f"""
    SELECT
        a.caller,
        a.end_ts   AS PrevEnd,
        b.start_ts AS NextStart,
        b.start_ts - a.end_ts AS IdleGap
    FROM static_data_store a
    JOIN static_data_store b
        ON  b.caller = a.caller
        AND b.start_ts > a.end_ts
    WHERE NOT EXISTS (
        SELECT 1
        FROM static_data_store c
        WHERE c.caller = a.caller
          AND c.start_ts > a.end_ts
          AND c.start_ts < b.start_ts
    )
    AND b.start_ts - a.end_ts > {IDLE_GAP_THRESHOLD_MS}
    ORDER BY a.caller, PrevEnd
    """
    
    result = spark.sql(query)
    print(f"Total gaps detected: {result.count()}")
    result.show(truncate=False)

class WatermarkListener(StreamingQueryListener):
    def __init__(self):
        self.last_triggered_wm = 0 
        self.trigger_interval  = TOTAL_WINDOW_SIZE - WATERMARK 

    def onQueryStarted(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        wm = event.progress.eventTime.get("watermark")

        if wm:
            dt = datetime.datetime.strptime(wm.replace('Z', '+0000'), "%Y-%m-%dT%H:%M:%S.%f%z")
            current_wm_epoch = int(dt.timestamp())

            if current_wm_epoch >= (self.last_triggered_wm + self.trigger_interval):
                self.last_triggered_wm = current_wm_epoch
                # Trigger the SQL analysis on the accumulated data
                sql_query(current_wm_epoch)

            print(
                f"[WM] batch={event.progress.batchId} wm={current_wm_epoch}s "
                f"processing_time={datetime.datetime.now().isoformat()} "
                f"rows={event.progress.numInputRows}"
            )

spark.streams.addListener(WatermarkListener())

# --- 4. Stream Join and Watermark ---
joined_df = (
    stream_with_id
        .join(csv_df, on="id", how="left")
        .select("timestamp", "value", "id", *csv_df.columns)
)

calls_with_ts = joined_df.withColumn(
    "event_ts",
    to_timestamp(col("end_ts") / 1000.0)
)

calls_with_wm = calls_with_ts.withWatermark("event_ts", "1 seconds") 

def update_static_view(batch_df, batch_id):
    """Collects new data and updates the global static view on the driver."""
    global GLOBAL_DATA_ACCUMULATOR
    
    new_rows = batch_df.collect()
    
    if new_rows:
        GLOBAL_DATA_ACCUMULATOR.extend(new_rows)
        spark.createDataFrame(GLOBAL_DATA_ACCUMULATOR, batch_df.schema).createOrReplaceTempView("static_data_store")
    
    if batch_id == 0:
        print("\n=== Stream started. Waiting for data accumulation and watermark advance... ===")


# --- 6. Final Query Execution ---
final_query = calls_with_wm.writeStream \
    .foreachBatch(update_static_view) \
    .option("checkpointLocation", "/tmp/spark/checkpoints/ratejoin_acc") \
    .outputMode("append") \
    .trigger(processingTime=f"{TRIGGER_INTERVAL_S} seconds") \
    .start()

final_query.awaitTermination()
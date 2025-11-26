import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.streaming import StreamingQueryListener

# --- Configuration ---
TOTAL_WINDOW_SIZE = 40
WATERMARK = 10
TRIGGER_INTERVAL_S = 1

# --- Spark Session and Data Loading (unchanged) ---
spark = (
    SparkSession.builder
        .appName("RateStreamJoinCSV_ACCUMULATOR")
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
    raise Exception("CSV file must contain 'start_ts' and 'end_ts' columns (in milliseconds) for the active call calculation.")

rate_df = (
    spark.readStream
        .format("rate")
        .option("rowsPerSecond", 10000)
        .option("rampUpTime", 0)
        .load()
)

stream_with_id = rate_df.withColumn("id", (col("value") % row_count) + 1)

# --- Global Accumulator Setup ---
# Global list to accumulate all processed stream data on the driver
GLOBAL_DATA_ACCUMULATOR = []
# Create a dummy initial schema for the static view
# We use the schema of the fully joined/transformed DataFrame for correctness
initial_schema = csv_df.withColumn("timestamp", col("id")).withColumn("value", col("id")).select("timestamp", "value", "id", *csv_df.columns).schema

# Initialize a static view for the batch query to read from on startup
spark.createDataFrame(GLOBAL_DATA_ACCUMULATOR, schema=initial_schema).createOrReplaceTempView("static_data_store")

# --- Batch Analysis Function (Reads from Accumulator View) ---
def sql_query(timestamp):
    window_start_s = timestamp - TOTAL_WINDOW_SIZE
    window_end_s = timestamp - WATERMARK
    
    window_start_ms = window_start_s * 1000
    window_end_ms = window_end_s * 1000

    print(f"\n[ANALYSIS] Triggering Batch SQL for Watermark: {timestamp}s")
    print(f"         Analyzing window (ms): [{window_start_ms}, {window_end_ms}]")
    
    # Query runs against the driver-populated static_data_store view
    query = f"""
    SELECT 
        count(*) as active_calls_count,
        {timestamp} as trigger_time,
        {window_start_ms} as window_start_ms,
        {window_end_ms} as window_end_ms
    FROM static_data_store  
    WHERE 
        end_ts >= ({window_start_ms})
        AND start_ts < ({window_end_ms})
    """
    
    result = spark.sql(query)
    result.show(truncate=False)

# --- Streaming Query Listener (FIXED: Trigger Logic) ---
class WatermarkListener(StreamingQueryListener):
    def __init__(self):
        # FIX: Initialize to 0 so the current stream's epoch time will advance it
        self.last_triggered_wm = 0 
        self.trigger_interval  = TOTAL_WINDOW_SIZE - WATERMARK 
        # Removed file I/O

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
                # CRITICAL: Trigger the analysis here
                sql_query(current_wm_epoch)

            print(
                f"[WM] batch={event.progress.batchId} wm={current_wm_epoch}s "
                f"processing_time={datetime.datetime.now().isoformat()} "
                f"rows={event.progress.numInputRows}"
            )

spark.streams.addListener(WatermarkListener())

# --- Stream Transformation ---
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

# --- ForeachBatch Function (UPDATED: Accumulates data on driver) ---
def update_static_view(batch_df, batch_id):
    """
    Collects the data from the micro-batch and appends it to the global list,
    then updates the static view for the SQL query.
    """
    global GLOBAL_DATA_ACCUMULATOR
    
    # CRITICAL: Collect the new rows to the driver
    new_rows = batch_df.collect()
    
    if new_rows:
        GLOBAL_DATA_ACCUMULATOR.extend(new_rows)
        # Update the static view that sql_query reads from
        # Note: This recreation forces the view to use the updated data
        spark.createDataFrame(GLOBAL_DATA_ACCUMULATOR, batch_df.schema).createOrReplaceTempView("static_data_store")
    
    if batch_id == 0:
        print("\n=== Stream started. Waiting for watermark to advance... ===")


# --- Final Query Execution ---
final_query = calls_with_wm.writeStream \
    .foreachBatch(update_static_view) \
    .option("checkpointLocation", "/tmp/spark/checkpoints/ratejoin_acc") \
    .outputMode("append") \
    .trigger(processingTime=f"{TRIGGER_INTERVAL_S} seconds") \
    .start()

final_query.awaitTermination()
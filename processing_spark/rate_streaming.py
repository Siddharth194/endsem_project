import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_timestamp, unix_timestamp
from pyspark.sql.streaming import StreamingQueryListener

spark = (
    SparkSession.builder
        .appName("RateStreamJoinCSV")
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
csv_df.show()

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

# Global list to accumulate all processed stream data on the driver (for demonstration only)
GLOBAL_DATA_ACCUMULATOR = []
# Initialize a dummy static view for the batch query to read from on startup
spark.createDataFrame(GLOBAL_DATA_ACCUMULATOR, csv_df.schema).createOrReplaceTempView("static_data_store")

TOTAL_WINDOW_SIZE = 40
WATERMARK = 10

def sql_query(timestamp, dataframe):
    window_start_ms = (timestamp - TOTAL_WINDOW_SIZE) * 1000
    window_end_ms = (timestamp - WATERMARK) * 1000

    print(f"\n[ANALYSIS] Triggering Batch SQL for Watermark: {timestamp}s")
    print(f"         Analyzing window (ms): [{window_start_ms}, {window_end_ms}]")
    
    query = f"""
    SELECT 
        count(*) as active_calls_count,
        {timestamp} as trigger_time,
        {window_start_ms} as window_start,
        {window_end_ms} as window_end
    FROM {dataframe}  
    WHERE 
        end_ts >= ({window_start_ms})
        AND start_ts < ({window_end_ms})
    """
    result = spark.sql(query)
    result.show(truncate=False)

class WatermarkListener(StreamingQueryListener):
    def __init__(self):
        self.last_triggered_wm = 1764079950000 + WATERMARK
        self.trigger_interval  = TOTAL_WINDOW_SIZE - WATERMARK 
        self.file = open("watermarks","w")

    def onQueryStarted(self, event):
        pass

    def onQueryTerminated(self, event):
        pass

    def onQueryProgress(self, event):
        wm = event.progress.eventTime.get("watermark")

        if wm:
            dt = datetime.datetime.strptime(wm.replace('Z', '+0000'), "%Y-%m-%dT%H:%M:%S.%f%z")
            current_wm_epoch = int(dt.timestamp())

            self.file.write(f"{current_wm_epoch}\n")

            print(
                f"[WM] batch={event.progress.batchId} wm={current_wm_epoch} "
                f"processing_time={datetime.datetime.now().isoformat()} "
                f"rows={event.progress.numInputRows}"
            )

spark.streams.addListener(WatermarkListener())

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

while (True):
    
import random
import time
import math
import argparse
from datetime import datetime, timedelta
import csv
import psycopg2

MAX_EVENTS = 100000
BI_SIGNAL_FILE =    "/home/siddharth/StreamingDataSystems/endsem_project/generation/bi_signal_2.csv"
MONO_SIGNAL_FILE =  "/home/siddharth/StreamingDataSystems/endsem_project/generation/mono_signal_2.csv"
TOWER_SIGNAL_FILE = "/home/siddharth/StreamingDataSystems/endsem_project/generation/tower_signal_2.csv"
DISPOSITIONS = ["connected", "busy", "failed", "connected"]

PG_HOST = "localhost"
PG_DB = "sds_project"
PG_USER = "postgres"
PG_PASSWORD = "root"
PG_PORT = 5432

INSERT = False


# ------------------ helpers ------------------
def get_random(min_val, max_val):
    return random.randint(min_val, max_val)

def get_random_disposition():
    return random.choice(DISPOSITIONS)

def normal_distribution_call_duration(threshold):
    if threshold <= 0:
        return 0
    mean = threshold / 2.0
    stddev = threshold / 4.0
    while True:
        u1 = random.random()
        u2 = random.random()
        z = math.sqrt(-2.0 * math.log(u1)) * math.cos(2 * math.pi * u2)
        value = mean + z * stddev
        if 1 <= value <= threshold:
            return int(value)

def add_time(timestamp_ms, interval_seconds):
    return timestamp_ms + interval_seconds * 1000


# ------------------ main record generator ------------------
def generate_tower_signal(tower_writer, tower_batch, data):
    num_towers = get_random(1, min(data["call_duration"], 10) if data["call_duration"] > 0 else 1)
    intervals = []

    for _ in range(num_towers - 1):
        while True:
            r = get_random(1, data["call_duration"] - 1)
            if r not in intervals:
                intervals.append(r)
                break

    intervals.append(data["call_duration"])
    intervals.sort()

    cur_start = data["start_timestamp"]

    for sec in intervals:
        tower = f"T{get_random(1, 10)}"
        end = add_time(data["start_timestamp"], sec)

        tower_writer.writerow([data["unique_id"], tower, cur_start, end])
        tower_batch.append((data["unique_id"], tower, cur_start, end))

        cur_start = end


def save_mono_signal(mono_writer, mono_batch, data):
    mono_writer.writerow([
        data["unique_id"],
        data["start_timestamp"],
        data["end_timestamp"],
        data["calling_party"],
        data["called_party"],
        data["disposition"],
        data["imei"]
    ])
    mono_batch.append((
        data["unique_id"],
        data["start_timestamp"],
        data["end_timestamp"],
        data["calling_party"],
        data["called_party"],
        data["disposition"],
        data["imei"]
    ))


def save_bi_signal(bi_writer, bi_batch, data):
    # event type 0 = start
    bi_writer.writerow([
        data["unique_id"], 0, data["calling_party"], data["called_party"],
        data["start_timestamp"], data["disposition"], data["imei"]
    ])
    bi_batch.append((
        data["unique_id"], 0, data["calling_party"], data["called_party"],
        data["start_timestamp"], data["disposition"], data["imei"]
    ))

    # event type 1 = end
    bi_writer.writerow([
        data["unique_id"], 1, data["calling_party"], data["called_party"],
        data["end_timestamp"], data["disposition"], data["imei"]
    ])
    bi_batch.append((
        data["unique_id"], 1, data["calling_party"], data["called_party"],
        data["end_timestamp"], data["disposition"], data["imei"]
    ))


def save_iteration_data(mono_writer, bi_writer, tower_writer,
                        mono_batch, bi_batch, tower_batch,
                        iteration, throughput, base_time, threshold, save=True):

    records = []

    for i in range(throughput):
        data = {}
        end_ms = get_random(0, 999)
        end_time = datetime.fromtimestamp(base_time).replace(microsecond=end_ms * 1000)
        data["end_timestamp"] = int(end_time.timestamp() * 1000)
        data["disposition"] = get_random_disposition()

        if data["disposition"] in ["busy", "failed"]:
            data["call_duration"] = 0
        else:
            data["call_duration"] = normal_distribution_call_duration(threshold)

        start_time = datetime.fromtimestamp(base_time - data["call_duration"])
        start_ms = end_ms if data["call_duration"] == 0 else get_random(1, end_ms or 1)

        data["start_timestamp"] = int(start_time.timestamp() * 1000)
        data["calling_party"] = 9000000000 + get_random(0, 999999999)
        data["called_party"] = 9000000000 + get_random(0, 999999999)
        data["imei"] = 100000000 + get_random(0, 9999999)
        data["unique_id"] = iteration * throughput + i + 1
        records.append(data)

    records.sort(key=lambda x: x["end_timestamp"])

    if save:
        for data in records:
            save_bi_signal(bi_writer, bi_batch, data)
            save_mono_signal(mono_writer, mono_batch, data)
            generate_tower_signal(tower_writer, tower_batch, data)
    else:
        return data


# ------------------ main ------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("throughput", type=int)
    parser.add_argument("iterations", type=int)
    parser.add_argument("threshold", type=int)
    args = parser.parse_args()

    # ---------- PostgreSQL connection ----------
    conn = psycopg2.connect(
        host=PG_HOST, database=PG_DB, user=PG_USER,
        password=PG_PASSWORD, port=PG_PORT
    )
    cur = conn.cursor()

    # ------------- Create tables once -------------
    cur.execute("""
        CREATE TABLE IF NOT EXISTS mono_signal (
            unique_id BIGINT,
            start_ts BIGINT,
            end_ts BIGINT,
            caller BIGINT,
            callee BIGINT,
            disposition TEXT,
            imei BIGINT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bi_signal (
            unique_id BIGINT,
            event_type INT,
            caller BIGINT,
            callee BIGINT,
            timestamp BIGINT,
            disposition TEXT,
            imei BIGINT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tower_signal (
            unique_id BIGINT,
            tower TEXT,
            start_ts BIGINT,
            end_ts BIGINT
        );
    """)
    conn.commit()

    current_time = int(time.time())

    print(current_time)

    with open(MONO_SIGNAL_FILE, 'w') as mono_file, \
         open(BI_SIGNAL_FILE, 'w') as bi_file, \
         open(TOWER_SIGNAL_FILE, 'w') as tower_file:
        
        print("csv files opened")

        mono_writer = csv.writer(mono_file)
        bi_writer = csv.writer(bi_file)
        tower_writer = csv.writer(tower_file)

        mono_writer.writerow(["unique_id", "start_ts", "end_ts", "caller", "callee", "disposition", "imei"])
        bi_writer.writerow(["unique_id", "event_type", "caller", "callee", "timestamp", "disposition", "imei"])
        tower_writer.writerow(["unique_id", "tower", "start_ts", "end_ts"])

        for i in range(args.iterations):

            mono_batch = []
            bi_batch = []
            tower_batch = []

            save_iteration_data(
                mono_writer, bi_writer, tower_writer,
                mono_batch, bi_batch, tower_batch,
                i + 1, args.throughput, current_time + i, args.threshold
            )

            # ------------ bulk insert ------------
            if (INSERT):
                cur.executemany(
                    "INSERT INTO mono_signal VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    mono_batch
                )
                cur.executemany(
                    "INSERT INTO bi_signal VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    bi_batch
                )
                cur.executemany(
                    "INSERT INTO tower_signal VALUES (%s,%s,%s,%s)",
                    tower_batch
                )

            conn.commit()
            print(".", end="", flush=True)

    print("\nDone.")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
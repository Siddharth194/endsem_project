#!/usr/bin/env python3
"""
flink_mono_csv_publish_latency.py

PyFlink DataStream job computing windowed call counts from mono CSV,
measuring publish latency per window as:
    publish_latency_ms = publish_time_ms - max_event_timestamp_in_window

Output CSV header:
  window_start_ms,count,max_ts,timer_ts,publish_time_ms,event_wait_ms,publish_latency_ms,max_type,max_ingest_ms

Usage:
  python3 flink_mono_csv_publish_latency.py --input generation/mono_signal.csv \
    --lead-ms 2000 --watermark-end-secs 2 --output mono_windows_out.csv --parallelism 1
"""
import argparse
import csv
import time
import os
import statistics

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types
from pyflink.datastream.functions import MapFunction, FlatMapFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.common.watermark_strategy import WatermarkStrategy

WINDOW_MS = 30_000  # 30 seconds windows (adjust if needed)


def find_min_ts(path):
    m = None
    with open(path, 'r') as f:
        r = csv.reader(f)
        next(r, None)
        for row in r:
            if not row or len(row) < 3:
                continue
            try:
                s = int(row[1]); e = int(row[2])
            except Exception:
                continue
            t = min(s, e)
            if m is None or t < m:
                m = t
    return m


class ParseAndShift(MapFunction):
    def __init__(self, shift_ms):
        self.shift_ms = int(shift_ms)

    def map(self, line: str):
        line = line.strip()
        if not line:
            return None
        if line.lower().startswith("unique_id"):
            return None
        parts = line.split(",")
        if len(parts) < 3:
            return None
        try:
            uid = int(parts[0])
            start_ts = int(parts[1]) + self.shift_ms
            end_ts = int(parts[2]) + self.shift_ms
            caller = int(parts[3]) if len(parts) > 3 and parts[3] != "" else 0
        except Exception:
            return None
        ingest_ts = int(time.time() * 1000)
        return {
            "unique_id": uid,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "caller": caller,
            "ingest_ts": ingest_ts
        }


class IntervalToWindowEvents(FlatMapFunction):
    def flat_map(self, interval):
        if interval is None:
            return
            yield
        try:
            s = int(interval['start_ts'])
            e = int(interval['end_ts'])
            ingest_ts = int(interval.get('ingest_ts', 0))
        except Exception:
            return
            yield
        if e < s:
            return
            yield
        first = (s // WINDOW_MS) * WINDOW_MS
        last = (e // WINDOW_MS) * WINDOW_MS
        w = first
        while w <= last:
            yield (w, 1, e, 'end', ingest_ts)
            w += WINDOW_MS


class WindowAccumulator(KeyedProcessFunction):
    def __init__(self, watermark_ms, output_file):
        super().__init__()
        self.watermark_ms = int(watermark_ms)
        self.output_file = output_file

    def open(self, runtime_context: RuntimeContext):
        self.state_desc = ValueStateDescriptor(
            "window_state",
            Types.TUPLE([Types.LONG(), Types.LONG(), Types.STRING(), Types.LONG()])
        )
        self.state = runtime_context.get_state(self.state_desc)
        outdir = os.path.dirname(self.output_file) or "."
        try:
            os.makedirs(outdir, exist_ok=True)
        except Exception:
            pass

    def process_element(self, value, ctx):
        # value: (window_start, inc, end_ts, type, ingest_ts)
        inc = int(value[1])
        end_ts = int(value[2])
        typ = value[3]
        ingest_ts = int(value[4]) if len(value) > 4 else int(time.time() * 1000)

        cur = self.state.value()
        if cur is None:
            cur = (0, 0, 'none', 0)
        
        curr_count = int(cur[0])
        curr_max_end = int(cur[1])
        curr_max_type = cur[2]
        curr_max_ingest = int(cur[3])

        new_count = curr_count + inc
        # Track the latest wall-clock time data arrived for this window
        new_max_ingest = max(curr_max_ingest, ingest_ts)

        if end_ts > curr_max_end:
            new_max_end = end_ts
            new_max_type = typ
        else:
            new_max_end = curr_max_end
            new_max_type = curr_max_type

        self.state.update((new_count, new_max_end, new_max_type, new_max_ingest))

        # Register timer: Event Time + Watermark Wait
        fire_ts = new_max_end + self.watermark_ms + 1
        ctx.timer_service().register_event_time_timer(fire_ts)

    def on_timer(self, timestamp, ctx):
        cur = self.state.value()
        if cur is None:
            return

        cnt = int(cur[0])
        max_end = int(cur[1])
        max_type = cur[2]
        max_ingest = int(cur[3])

        # Capture publish time (Wall Clock)
        publish_time = int(time.time() * 1000)

        # FIX: Calculate Processing Latency
        # Time from when the last piece of data arrived (ingest) to when we publish
        if max_ingest > 0:
            publish_latency_ms = publish_time - max_ingest
        else:
            # Fallback if ingest not tracked (shouldn't happen with correct map)
            publish_latency_ms = 0

        # Debugging metric: Timer deviation
        timer_ts = int(timestamp) if timestamp is not None else 0
        event_wait_ms = max(0, publish_time - timer_ts)

        line = "{window},{count},{max_end},{timer_ts},{publish_time},{event_wait},{publish_latency},{max_type},{max_ingest}\n".format(
            window=ctx.get_current_key(),
            count=cnt,
            max_end=max_end,
            timer_ts=timer_ts,
            publish_time=publish_time,
            event_wait=event_wait_ms,
            publish_latency=publish_latency_ms,
            max_type=max_type,
            max_ingest=max_ingest
        )

        try:
            with open(self.output_file, 'a') as f:
                f.write(line)
        except Exception:
            # Fallback to stdout if file write fails
            print(line, end='')

        self.state.clear()
        yield line

class EndTimestampAssigner:
    def extract_timestamp(self, value, record_timestamp):
        try:
            return int(value[2])
        except Exception:
            return int(time.time() * 1000)


def compute_stats_from_file(path):
    if not os.path.exists(path):
        return None
    import csv
    latencies = []
    with open(path, 'r') as f:
        rdr = csv.reader(f)
        hdr = next(rdr, None)
        if not hdr:
            return None
        hdr_norm = [h.strip().lower() for h in hdr]
        latency_idx = None
        # prefer publish_latency column
        for i, h in enumerate(hdr_norm):
            if 'publish_latency' in h or 'publish' in h and 'lat' in h:
                latency_idx = i
                break
        if latency_idx is None:
            # fallback to any 'lat' not 'event'
            for i, h in enumerate(hdr_norm):
                if 'lat' in h and 'event' not in h:
                    latency_idx = i
                    break
        for row in rdr:
            if not row:
                continue
            if latency_idx is None:
                # find last numeric column
                for j in range(len(row)-1, -1, -1):
                    try:
                        float(row[j])
                        latency_idx = j
                        break
                    except Exception:
                        continue
                if latency_idx is None:
                    continue
            try:
                lat = float(row[latency_idx])
                latencies.append(lat)
            except Exception:
                continue
    if not latencies:
        return None
    lat_sorted = sorted(latencies)
    avg = sum(lat_sorted) / len(lat_sorted)
    p50 = statistics.median(lat_sorted)
    p95 = lat_sorted[int(len(lat_sorted) * 0.95) - 1] if len(lat_sorted) > 1 else lat_sorted[-1]
    return {'count': len(lat_sorted), 'avg': avg, 'p50': p50, 'p95': p95}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="generation/mono_signal.csv")
    p.add_argument("--lead-ms", type=int, default=2000)
    p.add_argument("--watermark-end-secs", type=int, default=2)
    p.add_argument("--output", default="mono_windows_out.csv")
    p.add_argument("--parallelism", type=int, default=1)
    args = p.parse_args()

    min_ts = find_min_ts(args.input)
    if min_ts is None:
        raise SystemExit("no timestamps found in input")
    now_ms = int(time.time() * 1000)
    shift_ms = (now_ms + int(args.lead_ms)) - int(min_ts)
    print(f"Computed shift_ms={shift_ms}")

    lines = []
    with open(args.input, 'r') as f:
        for ln in f:
            ln = ln.rstrip("\n")
            if ln:
                lines.append(ln)

    header = "window_start_ms,count,max_ts,timer_ts,publish_time_ms,event_wait_ms,publish_latency_ms,max_type,max_ingest_ms\n"
    try:
        if os.path.exists(args.output):
            os.remove(args.output)
        with open(args.output, 'w') as fo:
            fo.write(header)
    except Exception as e:
        print("WARN: could not write header to output file:", e)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)

    src = env.from_collection(collection=lines, type_info=Types.STRING())

    parsed = src.map(ParseAndShift(shift_ms), output_type=Types.MAP(Types.STRING(), Types.LONG()))
    parsed = parsed.filter(lambda x: x is not None)

    per_window = parsed.flat_map(IntervalToWindowEvents(), output_type=Types.TUPLE([
        Types.LONG(), Types.INT(), Types.LONG(), Types.STRING(), Types.LONG()
    ]))

    wm = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(EndTimestampAssigner())
    timed = per_window.assign_timestamps_and_watermarks(wm)

    keyed = timed.key_by(lambda e: int(e[0]), key_type=Types.LONG())

    accumulator = WindowAccumulator(int(args.watermark_end_secs * 1000), args.output)
    _ = keyed.process(accumulator, output_type=Types.STRING())

    try:
        env.execute("mono_csv_publish_latency")
    except Exception as e:
        print("Job finished/terminated with exception:", e)

    stats = compute_stats_from_file(args.output)
    if stats:
        print(f"WINDOWS_EMITTED:{stats['count']}")
        print(f"AVG_LATENCY_MS:{stats['avg']:.2f}")
        print(f"P50_LATENCY_MS:{stats['p50']:.2f}")
        print(f"P95_LATENCY_MS:{stats['p95']:.2f}")
    else:
        print("No latency records found.")


if __name__ == "__main__":
    main()

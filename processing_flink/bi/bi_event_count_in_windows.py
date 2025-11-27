import argparse
import csv
import time
import os
import statistics
from collections import defaultdict, OrderedDict

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types
from pyflink.datastream.functions import MapFunction, KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor
from pyflink.common.watermark_strategy import WatermarkStrategy

WINDOW_MS = 30_000  # 30s windows


def preprocess_bi_to_window_aggregates(bi_csv_path, window_ms):
    """
    Read bi_signal.csv and compute per-window aggregates:
      window_start -> {
          'delta_sum': int,
          'max_event_ts': int
      }
    
    We do NOT compute ingest_ts here. Ingest time is relevant only when 
    the data actually enters the Flink pipeline.
    """
    window_map = {}  # window_start -> dict
    
    if not os.path.exists(bi_csv_path):
        return {}

    with open(bi_csv_path, 'r') as f:
        rdr = csv.reader(f)
        next(rdr, None) # skip header
        for row in rdr:
            if not row or len(row) < 5:
                continue
            try:
                # bi_signal cols: unique_id(0), event_type(1), caller(2), callee(3), timestamp(4)
                evtype = int(row[1])
                ts = int(row[4])  # event timestamp ms
            except Exception:
                continue
            
            # Semantics: Start(0) = +1, End(1) = -1
            delta = 1 if evtype == 0 else -1
            
            # Floor timestamp to window start
            win = (ts // window_ms) * window_ms
            
            ent = window_map.get(win)
            if ent is None:
                ent = {'delta_sum': 0, 'max_event_ts': 0}
                window_map[win] = ent
            
            ent['delta_sum'] += delta
            if ts > ent['max_event_ts']:
                ent['max_event_ts'] = ts

    return OrderedDict(sorted(window_map.items(), key=lambda x: x[0]))


class ParseAggregateMap(MapFunction):
    """
    Input line format: window_start,delta_sum,max_event_ts
    
    1. Parses the line.
    2. Applies shift_ms to window_start and max_event_ts (aligning historical data to 'now').
    3. Captures CURRENT wall-clock time as max_ingest_ms.
    """
    def __init__(self, shift_ms):
        self.shift_ms = int(shift_ms)

    def map(self, line: str):
        line = line.strip()
        if not line:
            return None
        parts = line.split(",")
        if len(parts) < 3:
            return None
        try:
            window_start = int(parts[0])
            delta_sum = int(parts[1])
            max_event_ts = int(parts[2])
        except Exception:
            return None
            
        current_ingest_ts = int(time.time() * 1000)

        return {
            'window_start': window_start + self.shift_ms,
            'delta_sum': delta_sum,
            'max_event_ts': max_event_ts + self.shift_ms,
            'max_ingest_ms': current_ingest_ts
        }


class EndTimestampAssigner:
    def extract_timestamp(self, value, record_timestamp):
        try:
            return int(value['max_event_ts'])
        except Exception:
            return int(time.time() * 1000)


class WindowDeltaEmitter(KeyedProcessFunction):

    def __init__(self, watermark_ms, output_file):
        super().__init__()
        self.watermark_ms = int(watermark_ms)
        self.output_file = output_file
        self.prev_state = None
        self.elem_state = None

    def open(self, runtime_context: RuntimeContext):
        self.prev_state_desc = ValueStateDescriptor("prev_active", Types.LONG())
        self.prev_state = runtime_context.get_state(self.prev_state_desc)
        
        # KEY: Long (Fire Timestamp), VALUE: Tuple (window_data)
        self.elem_state_desc = MapStateDescriptor("elem_state", Types.LONG(), Types.TUPLE([
            Types.LONG(), Types.INT(), Types.LONG(), Types.LONG()
        ]))
        self.elem_state = runtime_context.get_map_state(self.elem_state_desc)
        
        outdir = os.path.dirname(self.output_file)
        if outdir:
            try:
                os.makedirs(outdir, exist_ok=True)
            except Exception:
                pass

    def process_element(self, value, ctx):
        w = int(value['window_start'])
        delta = int(value['delta_sum'])
        max_event = int(value['max_event_ts'])
        max_ingest = int(value['max_ingest_ms'])

        # Calculate desired fire time
        base_fire_ts = max_event + self.watermark_ms + 1
        
        # Handle potential collisions (unlikely but safe)
        # If multiple windows map to the exact same ms, shift by 1ms until unique
        fire_ts = base_fire_ts
        while self.elem_state.contains(fire_ts):
            fire_ts += 1

        self.elem_state.put(fire_ts, (w, delta, max_event, max_ingest))

        # Register timer
        ctx.timer_service().register_event_time_timer(fire_ts)

    def on_timer(self, timestamp, ctx):
        # Retrieve the specific window data for this timer timestamp
        elem = self.elem_state.get(timestamp)
        if elem is None:
            return
            
        window_start = int(elem[0])
        delta_sum = int(elem[1])
        max_event_ts = int(elem[2])
        max_ingest_ms = int(elem[3])

        # 1. Retrieve previous cumulative count
        prev_active = self.prev_state.value()
        if prev_active is None:
            prev_active = 0
            
        # 2. Apply current window delta
        active = prev_active + delta_sum

        # 3. Calculate Latency
        publish_time = int(time.time() * 1000)
        

        publish_latency_ms = publish_time - max_ingest_ms
        
        # Event Wait: How long after the "timer" timestamp did we actually fire?
        timer_ts = int(timestamp) if timestamp is not None else 0
        event_wait_ms = max(0, publish_time - timer_ts)

        line = "{w},{count},{max_ts},{timer_ts},{pub},{event_wait},{pub_lat},{max_type},{ingest}\n".format(
            w=window_start,
            count=active,
            max_ts=max_event_ts,
            timer_ts=timer_ts,
            pub=publish_time,
            event_wait=event_wait_ms,
            pub_lat=publish_latency_ms, # This is now strictly Processing Latency
            max_type='delta',
            ingest=max_ingest_ms
        )

        try:
            with open(self.output_file, 'a') as fo:
                fo.write(line)
        except Exception:
            print(line, end='')

        # 4. Update global state for next window
        self.prev_state.update(active)
        
        # 5. Clear ONLY this specific window from state
        self.elem_state.remove(timestamp)

        yield line


def compute_stats_from_file(path):
    if not os.path.exists(path):
        return None
    latencies = []
    with open(path, 'r') as f:
        rdr = csv.reader(f)
        hdr = next(rdr, None)
        if not hdr:
            return None
        
        hdr_norm = [h.strip().lower() for h in hdr]
        latency_idx = None
        
        # Look for publish_latency_ms
        for i, h in enumerate(hdr_norm):
            if 'publish_latency' in h:
                latency_idx = i
                break
        
        if latency_idx is None:
            # Fallback
            for i, h in enumerate(hdr_norm):
                if 'lat' in h and 'event' not in h:
                    latency_idx = i
                    break

        if latency_idx is None:
            return None

        for row in rdr:
            if not row or len(row) <= latency_idx:
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


# ----------------- main -----------------
def main():
    p = argparse.ArgumentParser()
    p.add_argument("--input", default="generation/bi_signal.csv")
    p.add_argument("--lead-ms", type=int, default=2000, help="shift to bring events near now")
    p.add_argument("--watermark-end-secs", type=int, default=2, help="watermark wait in seconds")
    p.add_argument("--output", default="bi_windows_out.csv")
    p.add_argument("--parallelism", type=int, default=1)
    args = p.parse_args()

    # 1) Preprocess to per-window aggregates
    print(f"Preprocessing {args.input} to per-window aggregates...")
    window_aggs = preprocess_bi_to_window_aggregates(args.input, WINDOW_MS)
    
    if not window_aggs:
        print("No valid data found in input CSV.")
        return

    # Compute minimal timestamp to calculate shift
    min_win = min(window_aggs.keys())
    now_ms = int(time.time() * 1000)

    shift_ms = (now_ms + int(args.lead_ms)) - int(min_win)
    
    print(f"Computed shift_ms={shift_ms} (windows_count={len(window_aggs)})")
    
    # 2) Prepare lines for Flink
    lines = []
    for w, ent in window_aggs.items():
        # line: window_start,delta_sum,max_event_ts
        lines.append("{},{},{}".format(w, ent['delta_sum'], ent['max_event_ts']))

    # 3) Header
    header = "window_start_ms,count,max_ts,timer_ts,publish_time_ms,event_wait_ms,publish_latency_ms,max_type,max_ingest_ms\n"
    try:
        outdir = os.path.dirname(args.output)
        if outdir: 
            os.makedirs(outdir, exist_ok=True)
        with open(args.output, 'w') as fo:
            fo.write(header)
    except Exception as e:
        print("WARN: could not write header:", e)

    # 4) Flink Pipeline
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)
    
    if args.parallelism != 1:
        print("WARNING: --parallelism 1 is required for correct global cumulative sum.")

    src = env.from_collection(collection=lines, type_info=Types.STRING())

    # Map: Parse, Shift Time, Capture Ingest Time
    parsed = src.map(ParseAggregateMap(shift_ms), output_type=Types.MAP(Types.STRING(), Types.LONG()))
    parsed = parsed.filter(lambda x: x is not None)

    # Timestamps & Watermarks
    wm = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(EndTimestampAssigner())
    timed = parsed.assign_timestamps_and_watermarks(wm)

    # Global Keying (Constant Key 0) to ensure sequential processing for cumulative sum
    keyed = timed.key_by(lambda e: 0, key_type=Types.INT())

    # Process
    accumulator = WindowDeltaEmitter(int(args.watermark_end_secs * 1000), args.output)
    _ = keyed.process(accumulator, output_type=Types.STRING())

    try:
        env.execute("bi_csv_delta_windows")
    except Exception as e:
        print("Job finished/terminated with exception:", e)

    # 5) Stats
    stats = compute_stats_from_file(args.output)
    if stats:
        print(f"WINDOWS_EMITTED: {stats['count']}")
        print(f"AVG_LATENCY_MS:  {stats['avg']:.2f}")
        print(f"P50_LATENCY_MS:  {stats['p50']:.2f}")
        print(f"P95_LATENCY_MS:  {stats['p95']:.2f}")
    else:
        print("No latency records found.")


if __name__ == "__main__":
    main()
import pandas as pd

# ---------------------------
# 1. Load the CSV
# ---------------------------
df = pd.read_csv("generation/mono_signal.csv")

# Ensure timestamps are numeric
df["start_ts"] = df["start_ts"].astype(int)
df["end_ts"] = df["end_ts"].astype(int)

WINDOW = 30_000   # 30 seconds in ms; change to 30 if working in seconds

# ---------------------------
# 2. Build window ranges for each call
# ---------------------------
expanded_rows = []

for _, row in df.iterrows():
    uid = row["unique_id"]
    start_ts = row["start_ts"]
    end_ts = row["end_ts"]

    # floor to nearest 30s window
    win_start = (start_ts // WINDOW) * WINDOW

    # ceil to nearest 30s window
    win_end = (end_ts // WINDOW) * WINDOW

    # generate all window starts the call spans
    windows = range(win_start, win_end + WINDOW, WINDOW)

    for w in windows:
        expanded_rows.append((w, uid))

# Convert to DataFrame
expanded = pd.DataFrame(expanded_rows, columns=["window_start", "unique_id"])

# ---------------------------
# 3. Count distinct active calls per window
# ---------------------------
result = (
    expanded
    .drop_duplicates(["window_start", "unique_id"])   # IMPORTANT
    .groupby("window_start")["unique_id"]
    .nunique()
    .reset_index(name="active_calls")
)

print(result)

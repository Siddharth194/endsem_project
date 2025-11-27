import pandas as pd

# Load CSV
df = pd.read_csv("generation/mono_signal_2.csv")

# 1. Sort by end_ts
df = df.sort_values("end_ts").reset_index(drop=True)

# 2. Assign id = 0,1,2,... after sorting
df["id"] = range(len(df))

# Save back to CSV (optional)
df.to_csv("generation/mono_signal_2.csv", index=False)

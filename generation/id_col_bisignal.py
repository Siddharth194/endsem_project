import pandas as pd

df = pd.read_csv("generation/bi_signal.csv")
df = df.sort_values("timestamp").reset_index(drop=True)

df["id"] = range(len(df))

df.to_csv("generation/bi_signal.csv", index=False)

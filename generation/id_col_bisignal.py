import pandas as pd

df = pd.read_csv("generation/bi_signal_2.csv")
df = df.sort_values("timestamp").reset_index(drop=True)

df["id"] = range(len(df))

df.to_csv("generation/bi_signal_2.csv", index=False)

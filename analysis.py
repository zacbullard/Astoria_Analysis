import pandas as pd

df = pd.read_pickle("/home/z/ViaChallenge/sample_data_df")

print(type(df['pickup_datetime'].iloc[1]))


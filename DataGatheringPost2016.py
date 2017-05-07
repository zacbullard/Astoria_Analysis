import pandas as pd
import glob as glob

#df = pd.read_pickle('../results/yellow_taxi1')
all_trip_files = glob.glob('../results/*')
df_list = []
for a_file in all_trip_files:

    df = pd.read_pickle(a_file)
    df_list.append(df)

df = pd.concat(df_list)
print(df.size)


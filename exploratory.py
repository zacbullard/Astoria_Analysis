import pandas as pd
import glob as glob
from geopy.geocoders import Nominatim

geolocator = Nominatim(timeout=10)

trippath = '../small_trip_data'
#farepath = '../small_trip_fare'
pickup_neighborhoods = ['Clinton','Chelsea']
dropoff_neighborhoods = ['Clinton','Chelsea']

print("Starting program...")
    
def clean_dfs():
    pass

def findZip(series):
    pickup_location = geolocator.reverse(str(series['pickup_latitude']) + ',' + str(series['pickup_longitude']))
    dropoff_location = geolocator.reverse(str(series['dropoff_latitude']) + ',' + str(series['dropoff_longitude']))
    series['pickup_zip'] = int(pickup_location.raw['address']['postcode'])
    series['pickup_neighborhood'] = pickup_location.raw['address']['neighbourhood']
    series['dropoff_zip'] = int(dropoff_location.raw['address']['postcode'])
    series['dropoff_neighborhood'] = dropoff_location.raw['address']['neighbourhood']
    return series

all_trip_files = glob.glob(trippath + '/*.csv')
#all_fare_files = glob.glob(farepath + '/*.csv')

frames_list = []

for a_file in all_trip_files:
    #Gathering Trip Data
    dft = pd.read_csv(a_file,index_col=None, header=0)
    dft.columns = dft.columns.str.strip() #stripping whitespace from headers
    dft = dft.apply(findZip,axis = 1)   
    dft = dft[(dft['pickup_neighborhood'].isin(pickup_neighborhoods)) 
        | (dft['dropoff_neighborhood'].isin(dropoff_neighborhoods))]

    #Gathering Fare Data
    dff = pd.read_csv(a_file.replace('data','fare'),index_col=None, header=0)
    dff.columns = dff.columns.str.strip() #stripping whitespace from headers
    dft = dft.join(dff[['payment_type','fare_amount','surcharge','mta_tax','tip_amount','tolls_amount','total_amount']], how = 'inner')

    frames_list.append(dft)

df = pd.concat(trip_frames_list)
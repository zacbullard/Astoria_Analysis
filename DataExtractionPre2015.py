#Dependencies: GDAL/OGR, pandas

import pandas as pd
import glob as glob
import ogr
import time

start = time.time()

trippath = '../single_trip_data'
taxiShapefilePath = '../taxi_zones/taxi_zones.shp'
taxiZoneLookupPath = '../taxi_zone_lookup.csv'

#Regions of interest as defined by nyc.gov's taxi zoning
Astoria = ['Astoria','Astoria Park']
Midtown = ['Midtown Center','Midtown North','Midtown South','Midtown East']
UpperEastSide = ['Upper East Side North','Upper East Side South']

UpperManhattanLat = 40.76
NYCN = 40.92
NYCS = 40.49
NYCW = -74.26
NYCE = -73.69


print("Starting program...")
    
def clean_dfs():
    pass
    
#Reverse geocoding using the nyc.gov provided shapefile and OGR
#OGR: OpenGIS Simple Features Reference Implementation
#GDAL: Geospatial Data Abstraction Library
def findZipOGR(series, lyr_in, idx_reg, zoneLookup, ctran):
    try:
        pickup_location = reverseGeocode(series['pickup_longitude'], series['pickup_latitude'], lyr_in, idx_reg, zoneLookup, ctran)
        dropoff_location = reverseGeocode(series['dropoff_longitude'], series['dropoff_latitude'], lyr_in, idx_reg, zoneLookup, ctran)
        series['pickup_borough'] = pickup_location[0]
        series['pickup_neighborhood'] = pickup_location[1]
        series['dropoff_borough'] = dropoff_location[0]
        series['dropoff_neighborhood'] = dropoff_location[1]
    except:
        series['pickup_borough'] = 'NA'
        series['pickup_neighborhood'] = 'NA'
        series['dropoff_borough'] = 'NA'
        series['dropoff_neighborhood'] = 'NA'
    return series
    
def reverseGeocode(lon, lat, lyr_in, idx_reg, zoneLookup, ctran):
    #See if our point is outside the outermost bounds of NYC
    if (lon < NYCW) or (NYCE < lon) or (lat < NYCS) or (NYCN < lat):
            return ('NA','NA')

    #Transform incoming longitude/latitude to the shapefile's projection
    [lon,lat,z]=ctran.TransformPoint(lon,lat)

    #Create a point
    pt = ogr.Geometry(ogr.wkbPoint)
    pt.SetPoint_2D(0, lon, lat)

    #Set up a spatial filter such that the only features we see when we
    #loop through "lyr_in" are those which overlap the point defined above
    lyr_in.SetSpatialFilter(pt)

    #Loop through the overlapped features and display the field of interest
    for feat_in in lyr_in:
        NYCLocation = int(feat_in.GetFieldAsString(idx_reg))
        borough = zoneLookup.iloc[NYCLocation]['Borough']
        neighborhood = zoneLookup.iloc[NYCLocation]['Zone']
        return (borough,neighborhood)
        
zoneLookup = pd.read_csv(taxiZoneLookupPath, index_col = 0, header=0)     

#Preparing the necessary overhead to reverse geocode from the NYC shapefile 
ds_in = ogr.Open(taxiShapefilePath) #Get the contents of the shape file
lyr_in = ds_in.GetLayer(0)    #Get the shape file's first layer
idx_reg = lyr_in.GetLayerDefn().GetFieldIndex("LocationID") #Put the title of the field you are interested in here
#The following assumes that the latitude longitude is in WGS84
#This is identified by the number "4326", as in "EPSG:4326"
#We will create a transformation between this and the shapefile's
#project, whatever it may be
geo_ref = lyr_in.GetSpatialRef()
point_ref = ogr.osr.SpatialReference()
point_ref.ImportFromEPSG(4326)
ctran = ogr.osr.CoordinateTransformation(point_ref,geo_ref)

all_trip_files = glob.glob(trippath + '/*.csv')
#frames_list = []

print('It took {0:0.1f} seconds to initialize.'.format(time.time() - start))

fileNum = 0
for a_file in all_trip_files:
    print("reading in " + a_file + "...")
    fileNum += 1
    #Gathering Trip Data
    dft = pd.read_csv(a_file,index_col=False, header=0, usecols=[5,6,7,8,9,10,11,12,13])
    dft.columns = dft.columns.str.strip() #stripping whitespace from headers
    print('It took {0:0.1f} seconds to read that file'.format(time.time() - start))

    dft = dft.apply(findZipOGR,args=(lyr_in, idx_reg, zoneLookup, ctran),axis = 1)   
    dft = dft[
        ((dft['pickup_neighborhood'].isin(Astoria)) #From Astoria to Manhattan
        & (dft['dropoff_borough'].isin(['Manhattan']))) 
        |
        ((dft['dropoff_neighborhood'].isin(Astoria)) #From Manhattan to Astoria 
        & (dft['pickup_borough'].isin(['Manhattan']))) 
        |
        ((dft['dropoff_neighborhood'].isin(Astoria)) #Within Astoria 
        & (dft['pickup_neighborhood'].isin(Astoria))) 
        |
        ((dft['pickup_neighborhood'].isin(['LaGuardia Airport'])) #From LGA to Astoria 
        & (dft['dropoff_neighborhood'].isin(Astoria))) 
        |
        ((dft['dropoff_neighborhood'].isin(['LaGuardia Airport'])) #From Astoria to LGA
        & (dft['pickup_neighborhood'].isin(Astoria))) 
        |
        ((dft['pickup_neighborhood'].isin(['LaGuardia Airport'])) #From LGA to Upper Manhattan 
        & (dft['dropoff_latitude'] >= UpperManhattanLat)
        & (dft['dropoff_borough'].isin(['Manhattan']))) 
        |
        ((dft['dropoff_neighborhood'].isin(['LaGuardia Airport'])) #From Upper Manhattan to LGA
        & (dft['pickup_latitude'] >= UpperManhattanLat)
        & (dft['dropoff_borough'].isin(['Manhattan'])))
        |
        ((dft['pickup_neighborhood'].isin(UpperEastSide)) #From Upper East Side to Midtown
        & (dft['dropoff_neighborhood'].isin(Midtown))) 
        |
        ((dft['dropoff_neighborhood'].isin(UpperEastSide)) #From Midtown to Upper East Side 
        & (dft['pickup_neighborhood'].isin(Midtown))) 
        |
        ((dft['dropoff_neighborhood'].isin(UpperEastSide)) #Within Upper East Side 
        & (dft['pickup_neighborhood'].isin(UpperEastSide)))
        ]
    print('It took {0:0.1f} seconds to process that file'.format(time.time() - start))
    #Gathering Fare Data
    dff = pd.read_csv(a_file.replace('data','fare'),index_col=False, header=0,usecols=[4,5,6,7,8,9,10])
    dff.columns = dff.columns.str.strip() #stripping whitespace from headers
    dft = dft.join(dff, how = 'inner')
    dft.to_pickle(str(fileNum) + "_df")

    print('It took {0:0.1f} seconds to add in fare data'.format(time.time() - start))
    #frames_list.append(dft)

#df = pd.concat(frames_list)

print('It took {0:0.1f} seconds to complete the run'.format(time.time() - start))
#df.to_pickle("astoria_trips_df")
#df.to_csv("astoria_trips.csv")
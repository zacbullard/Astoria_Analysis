#Dependencies: GDAL/OGR, pandas

import pandas as pd
import glob as glob
import ogr
import time
import os
import sys

start = time.time()
filePath = os.path.dirname(os.path.realpath(__file__))

trippath = filePath + '/../small2016TaxiData/'
taxiShapefilePath = filePath + '/../taxi_zones/taxi_zones.shp'
taxiZoneLookupPath = filePath + '/../taxi_zone_lookup.csv'

#Regions of interest as defined by nyc.gov's taxi zoning
Astoria = ['Astoria','Astoria Park']
Midtown = ['Midtown Center','Midtown North','Midtown South','Midtown East']
UpperEastSide = ['Upper East Side North','Upper East Side South']

UpperManhattanLat = 40.76
NYCN = 40.92
NYCS = 40.49
NYCW = -74.26
NYCE = -73.69

def cleanData(monthrange):
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
    
    
    readData(trippath, 'greenTaxi'+monthrange,lyr_in, idx_reg, zoneLookup, ctran)
    readData(trippath, 'yellowTaxi'+monthrange,lyr_in, idx_reg, zoneLookup, ctran)
    #gdf = readData(trippath, 'green_taxi',lyr_in, idx_reg, zoneLookup, ctran)
    #ydf = readData(trippath, 'yellow_taxi',lyr_in, idx_reg, zoneLookup, ctran)
    #return pd.concat([gdf,ydf])
    
    print('It took {0:0.1f} seconds to initialize.'.format(time.time() - start))
    
        
def readData(trippath,folder,lyr_in, idx_reg, zoneLookup, ctran):
    
    all_trip_files = glob.glob(trippath + folder + '/*.csv')
    
    fileNum = 0
    #dfList = []
    for a_file in all_trip_files:
        
        fileNum += 1
        print("reading in " + a_file + "...")
        #Gathering Trip Data. Unfortunately yellow and green cabs have different data formats.
        df = pd.DataFrame()
        if 'green' in folder:
            df = pd.read_csv(a_file,index_col=False, header=0, usecols=[1,2,5,6,7,8,9,10,11,12,13,14,15,17,18,19])
            df.columns = ['pickup_datetime','dropoff_datetime','pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude','passenger_count','trip_distance','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','payment_type']
        elif 'yellow' in folder:
            df = pd.read_csv(a_file,index_col=False, header=0, usecols=[1,2,3,4,5,6,9,10,11,12,13,14,15,16,17,18])
            df.columns = ['pickup_datetime','dropoff_datetime','passenger_count','trip_distance','pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude','payment_type','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount']
        else:
            raise Exception("ERROR: cannot find folder of name " + folder)
        
        #df.columns = df.columns.str.strip().lower() #stripping whitespace from headers
        print('It took {0:0.1f} seconds to read that file'.format(time.time() - start))
    
        df = df.apply(findZipOGR,args=(lyr_in, idx_reg, zoneLookup, ctran),axis = 1)   
        df = df[
            ((df['pickup_neighborhood'].isin(Astoria)) #From Astoria to Manhattan
            & (df['dropoff_borough'].isin(['Manhattan']))) 
            |
            ((df['dropoff_neighborhood'].isin(Astoria)) #From Manhattan to Astoria 
            & (df['pickup_borough'].isin(['Manhattan']))) 
            |
            ((df['dropoff_neighborhood'].isin(Astoria)) #Within Astoria 
            & (df['pickup_neighborhood'].isin(Astoria))) 
            |
            ((df['pickup_neighborhood'].isin(['LaGuardia Airport'])) #From LGA to Astoria 
            & (df['dropoff_neighborhood'].isin(Astoria))) 
            |
            ((df['dropoff_neighborhood'].isin(['LaGuardia Airport'])) #From Astoria to LGA
            & (df['pickup_neighborhood'].isin(Astoria))) 
            |
            ((df['pickup_neighborhood'].isin(['LaGuardia Airport'])) #From LGA to Upper Manhattan 
            & (df['dropoff_latitude'] >= UpperManhattanLat)
            & (df['dropoff_borough'].isin(['Manhattan']))) 
            |
            ((df['dropoff_neighborhood'].isin(['LaGuardia Airport'])) #From Upper Manhattan to LGA
            & (df['pickup_latitude'] >= UpperManhattanLat)
            & (df['dropoff_borough'].isin(['Manhattan'])))
            |
            ((df['pickup_neighborhood'].isin(UpperEastSide)) #From Upper East Side to Midtown
            & (df['dropoff_neighborhood'].isin(Midtown))) 
            |
            ((df['dropoff_neighborhood'].isin(UpperEastSide)) #From Midtown to Upper East Side 
            & (df['pickup_neighborhood'].isin(Midtown))) 
            |
            ((df['dropoff_neighborhood'].isin(UpperEastSide)) #Within Upper East Side 
            & (df['pickup_neighborhood'].isin(UpperEastSide)))
            ]
        
        print('It took {0:0.1f} seconds to process that file'.format(time.time() - start))
        df.to_pickle(folder+str(fileNum))
        #dfList.append(df)
        
    #return pd.concat(dfList)

    print('It took {0:0.1f} seconds to add in fare data'.format(time.time() - start))
    #frames_list.append(df)
    
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


if __name__ == '__main__':
    print("Starting program...")

    cleanData(sys.argv[1])
    #df = cleanData()
    #df.to_pickle("astoria_trips_df")
    print('It took {0:0.1f} seconds to complete the run'.format(time.time() - start))
    
    #df.to_csv("astoria_trips.csv")
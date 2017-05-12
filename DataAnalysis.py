#Calculates commuter efficiency and potential
import pandas as pd
import numpy as np
import glob as glob
import math
import time
import os

start = time.time()
filePath = os.path.dirname(os.path.realpath(__file__))
cleanedTripPath = filePath + '/../reverseGeocachedData/*'

#The following variables represent static estimates or averages.
#With the addition of more data and modeling of each parameter, even greater accuracy can be reached.

#Regions of interest as defined by nyc.gov's taxi zoning
Astoria = ['Astoria','Astoria Park']
Midtown = ['Midtown Center','Midtown North','Midtown South','Midtown East']
UpperEastSide = ['Upper East Side North','Upper East Side South']

#Lower cut-off latitude for destinations requiring the Queensboro bridge or Kennedy F. Kennedy Bridge from LGA,
#Which necessitates traveling through/near Astoria, as approximated by Google Maps routing.
UpperManhattanLat = 40.76

#Our goal for number of passengers per carpool
carpoolGoal = 3

#Our estimate for the mean time in minutes it takes for a via-cle to gather up a carpoolGoal worth of people 
intervalWeekMinutes = 15

#Our estimate for mean time in minutes it takes to drive from LGA to Astoria
LGAtoAstoria = 20

#Our estimate for mean time in minutes it takes to drive from Upper Manhattan to LGA
UpperManhattanToAstoria = 30

#grams of CO2 per driven mile, as per the EPA
#https://www.epa.gov/sites/production/files/2016-02/documents/420f14040a.pdf
CO2perMile = 411

def readFiles():
   
    all_trip_files = glob.glob(cleanedTripPath)
    df_list = []
    for a_file in all_trip_files:
    
        df = pd.read_pickle(a_file)
        df_list.append(df)
    
    df = pd.concat(df_list)
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'])
    df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'])
    return df

#Separate out the different destination or arrival neighborhoods,
#and perform analysis on each.
def findCommutes(df):
    dfList = []
    #We groupby both the pickup and destination neighborhoods, effectively looping through all routes
    gp = df.groupby([df.pickup_neighborhood,df.dropoff_neighborhood])
    
    for key, item in gp:
        print('\n' + 'Finding trips between: ' + str(key) + '...' + key[0] + ' ' + key[1])
   
        transformed_df = transformData(gp.get_group(key))
        transformed_df['pickup_neighborhood'] = key[0]
        transformed_df['dropoff_neighborhood'] = key[1]
        #transformed_df['pickup_borough'] = transformed_df.iloc[0].pickup_borough
        #transformed_df['dropoff_borough'] = transformed_df.iloc[0].dropoff_borough
   
        dfList.append(transformed_df)

    return pd.concat(dfList)

#Given a dataframe of of taxi trips between two specific boroughs, clusters the ride grouped on weekday and given minute interval.
def transformData(df):
    #Latitude and longitue are irrelevant now that we aggregating data
    df = df.drop(['pickup_longitude','pickup_latitude','dropoff_longitude','dropoff_latitude'], axis = 1)
    #Calculate minutes since 12:00am Monday
    df = df.apply(calcWeekMinutes,axis = 1)
    #The following is a bit tricky, first we groupby week, then we groupby some interval of minutes throughout the week.
    gp = df.groupby([pd.TimeGrouper('W',key = 'pickup_datetime'),pd.cut(df.week_minutes,np.arange(0,7*24*60+intervalWeekMinutes,intervalWeekMinutes))])
    df = gp.sum()
    df['taxi_count'] = gp.size() #Retrieve the number of rows in our grouped object for the taxi count
    df = df.fillna(0)
    df = df.apply(calcCarpools,axis = 1)
    return(df)

#Let's crunch some metadata
def analyzeMetaData(df):
    
    df.total_taxis = int(df.taxi_count.sum())
    df.total_carpools = int(df.carpool_count.sum())
    df.car_reduction_ratio = df.total_carpools/df.total_taxis
    df.miles_reduction = df.car_reduction_ratio*df.trip_distance.sum()
    df.CO2_reduction = (df.miles_reduction * CO2perMile)/907185 #907185 grams per ton
    df.taxi_mean_passenger = df.passenger_count.sum()/df.total_taxis
    df.total_fare = df.fare_amount.sum()
    
    print('Total Taxi Count: ' + str(df.total_taxis) + '\n' + 
          'Total Carpool Count: ' + str(df.total_carpools) + '\n' +
          'Car Reduction Ratio: ' + str('%.2f' % df.car_reduction_ratio) + '\n' +
          'Mile Reduction: ' + str('%.2f' % df.miles_reduction) + '\n' +
          'CO2 Reduction (Tons): ' + str('%.2f' % df.CO2_reduction) + '\n' +
          'Total Fare: ' + str('%.2f' % df.total_fare) + '\n' +
          'Mean Passengers Per Taxi: ' + str('%.2f' % df.taxi_mean_passenger))
    return df

#Given a time, we want to return the minutes since 12:00am Monday, or week_minutes as I call it.
def calcWeekMinutes(series):
    
    date = series.pickup_datetime
    series['week_minutes'] = date.dayofweek*24*60 + date.hour*60 + date.minute
    return series

def calcCarpools(series):
    
    if (series.taxi_count == 0):
        series['carpool_count'] = 0   
    #If there is exactly one taxi, we cannot carpool better than that.
    elif (series.taxi_count == 1):
        series['carpool_count'] = 1 
    else:
        carpoolCount = math.floor(series.passenger_count/carpoolGoal)
        #Even if there are less people than our goal, we are still obligated to pick them up.
        if carpoolCount == 0:
            series['carpool_count'] = 1
        else:
            series['carpool_count'] = carpoolCount
        
    return series

#To calculate the extra carpooling opportunities afforded by traffic betwixt LGA and Upper Manhattan,
#we merge commuters starting from LGA and Astoria heading to Manhattan,
#but time-shift the fliers by the time it takes them to get near Astoria.
def findCommuteAirport(dfLU,dfUL,dfAM,dfMA,dfLA,dfAL):

    #The following trips do not have a pickup or dropoff in Astoria, but do pass through it.
    dfLU.pickup_datetime = dfLU.pickup_datetime + pd.DateOffset(minutes=LGAtoAstoria)
    dfLU.pickup_neighborhood = 'Astoria'
   
    dfUL.pickup_datetime = dfUL.pickup_datetime + pd.DateOffset(minutes=UpperManhattanToAstoria)
    dfUL.pickup_neighborhood = 'Astoria'
    
    return pd.concat([dfLU,dfUL,dfAM,dfMA,dfLA,dfAL])


if __name__ == '__main__':
    df = readFiles()
    
    #We analyze the data pertaining to the neighborhoods in question.
    
    dfAA = df[((df['dropoff_neighborhood'].isin(Astoria)) #Within Astoria 
            & (df['pickup_neighborhood'].isin(Astoria)))] 
    
    dfAM = df[((df['pickup_neighborhood'].isin(Astoria)) #From Astoria to Manhattan
            & (df['dropoff_borough'].isin(['Manhattan'])))] 
    
    dfMA = df[((df['dropoff_neighborhood'].isin(Astoria)) #From Manhattan to Astoria 
            & (df['pickup_borough'].isin(['Manhattan'])))]       
    
    dfLU = df[((df['pickup_neighborhood'].isin(['LaGuardia Airport'])) #From LGA to Upper Manhattan 
            & (df['dropoff_latitude'] >= UpperManhattanLat)
            & (df['dropoff_borough'].isin(['Manhattan'])))]
    
    dfUL = df[((df['dropoff_neighborhood'].isin(['LaGuardia Airport'])) #From Upper Manhattan to LGA
            & (df['pickup_latitude'] >= UpperManhattanLat)
            & (df['pickup_borough'].isin(['Manhattan'])))]
          
    dfLA = df[((df['pickup_neighborhood'].isin(['LaGuardia Airport'])) #From LGA to Astoria 
            & (df['dropoff_neighborhood'].isin(Astoria)))] 

    dfAL = df[((df['dropoff_neighborhood'].isin(['LaGuardia Airport'])) #From Astoria to LGA
            & (df['pickup_neighborhood'].isin(Astoria)))]
    
    dfAAll = findCommuteAirport(dfLU,dfUL,dfAM,dfMA,dfLA,dfAL) #From Astoria to Manhattan and Manhattan to Astoria, but with LGA traffic

    dfUMid = df[((df['pickup_neighborhood'].isin(UpperEastSide)) #From Upper East Side to Midtown
            & (df['dropoff_neighborhood'].isin(Midtown)))] 
         
    dfMidU = df[((df['dropoff_neighborhood'].isin(UpperEastSide)) #From Midtown to Upper East Side 
            & (df['pickup_neighborhood'].isin(Midtown)))] 
         
    dfUU = df[((df['dropoff_neighborhood'].isin(UpperEastSide)) #Within Upper East Side 
            & (df['pickup_neighborhood'].isin(UpperEastSide)))]
    
    del df

    dfDict = {'AA':dfAA,
              'AM':dfAM,
              'MA':dfMA,
              'AAll':dfAAll,
              'UMid':dfUMid,
              'MidU':dfMidU,
              'UU':dfUU}
    
    del dfAA, dfAM, dfMA, dfLU, dfUL, dfLA, dfAL, dfAAll, dfUMid, dfMidU, dfUU
    
    print('It took {0:0.1f} seconds to read in the data frames'.format(time.time() - start))
    
    for dfkey in dfDict:
        print('\n' + 'Analyzing commutes: ' + dfkey + '...')
        df = findCommutes(dfDict[dfkey])
        df = analyzeMetaData(df)
        df.to_pickle(filePath +'/analyzedData/' + dfkey)
        dfDict[dfkey] = df
        print('It took {0:0.1f} seconds to analyze that file'.format(time.time() - start))
    
    #Finally print a summary of the metadata
    for key in dfDict:
        df = dfDict[key]
        print('\n\n\n' + key + '\n')
        print('Total Taxi Count: ' + str(df.total_taxis) + '\n' + 
          'Total Carpool Count: ' + str(df.total_carpools) + '\n' +
          'Car Reduction Ratio: ' + str('%.2f' % df.car_reduction_ratio) + '\n' +
          'Mile Reduction: ' + str('%.2f' % df.miles_reduction) + '\n' +
          'CO2 Reduction (Tons): ' + str('%.2f' % df.CO2_reduction) + '\n' +
          'Total Fare: ' + str('%.2f' % df.total_fare) + '\n' +
          'Mean Passengers Per Taxi: ' + str('%.2f' % df.taxi_mean_passenger) + '\n' +
          str(df.total_taxis) + '\n' + 
          str(df.total_carpools) + '\n' +
          str('%.2f' % df.car_reduction_ratio) + '\n' +
          str('%.2f' % df.miles_reduction) + '\n' +
          str('%.2f' % df.CO2_reduction) + '\n' +
          str('%.2f' % df.total_fare) + '\n' +
          str('%.2f' % df.taxi_mean_passenger))
    
    print('It took {0:0.1f} seconds to finish the run.'.format(time.time() - start))
Astoria Carpooling Analysis
==============

Required Packages
--------------
Pandas, OGR, Python 3.4

Data Extraction
--------------
**Command line argument for extraction scripts:** path (relative to the script) of the raw taxi .csv files

.csv files of taxi trips were downloaded from the following sources:
- Pre 2015: http://www.andresmh.com/nyctaxitrips/
- Post 2015: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml 

Pandas was used for database management. Reading of the .csv files has to be done carefully, as the taxi data providers changed in 2015, thus presenting different formats. There are extraction scripts provided for each format.

Latitude and longitude for pickups/dropoffs were used to determine the corresponding neighborhood and borough. This was accomplished by reverse geocaching the lat/lon using the nyc.gov provided shapefile and the OGR python package.

Local reverse geocatching is used, as most (free) server-side reverse geocatching services will reject your IP after too many requests. Even performed locally, this process represents the most time-consuming process in the analysis.
*Note that starting July 2016, latitude and longitude are no-longer reported,* instead being replaced with a number corresponding to a neighborhood lookup table.

After reverse geocaching, to pare down file size we only save trips relevant to the Via challenge. We lump together nyc taxi zones as the following:
- Astoria = ['Astoria','Astoria Park']
- Midtown = ['Midtown Center','Midtown North','Midtown South','Midtown East']
- UpperEastSide = ['Upper East Side North','Upper East Side South']

Furthermore, we save LGA trips that begin/end in Astoria, and also if they have a Manhattan beginning/end above a latitude of 40.76. This is due to the fact that Northerly Manhattan<->LGA trips require traversing either the Queensboro or Kennedy bridge, the entrances of which are near Astoria. That particular latitude was chosen as an estimate, using the routes chosen by Google Maps.

The final processed dataframe is saved as a pickled Pandas dataframe for later analysis. 

Data Analysis
--------------

The freshly reverse geocached data can now be separated based on the following trip classifications:
    
    #Within Astoria
    dfAA = df[((df['dropoff_neighborhood'].isin(Astoria))  
            & (df['pickup_neighborhood'].isin(Astoria)))] 
    
    #From Astoria to Manhattan
    dfAM = df[((df['pickup_neighborhood'].isin(Astoria)) 
            & (df['dropoff_borough'].isin(['Manhattan'])))] 
    
    #From Manhattan to Astoria 
    dfMA = df[((df['dropoff_neighborhood'].isin(Astoria)) 
            & (df['pickup_borough'].isin(['Manhattan'])))]       
    
    #From LGA to Upper Manhattan
    dfLU = df[((df['pickup_neighborhood'].isin(['LaGuardia Airport']))  
            & (df['dropoff_latitude'] >= UpperManhattanLat)
            & (df['dropoff_borough'].isin(['Manhattan'])))]
    
    #From Upper Manhattan to LGA
    dfUL = df[((df['dropoff_neighborhood'].isin(['LaGuardia Airport'])) 
            & (df['pickup_latitude'] >= UpperManhattanLat)
            & (df['pickup_borough'].isin(['Manhattan'])))]
        
    #From LGA to Astoria  
    dfLA = df[((df['pickup_neighborhood'].isin(['LaGuardia Airport']))  
            & (df['dropoff_neighborhood'].isin(Astoria)))] 

    #From Astoria to LGA
    dfAL = df[((df['dropoff_neighborhood'].isin(['LaGuardia Airport'])) 
            & (df['pickup_neighborhood'].isin(Astoria)))]
    
    #From Astoria to Manhattan and Manhattan to Astoria, but with added LGA traffic
    dfAAll = findCommuteAirport(dfLU,dfUL,dfAM,dfMA,dfLA,dfAL) 

    #From Upper East Side to Midtown
    dfUMid = df[((df['pickup_neighborhood'].isin(UpperEastSide)) 
            & (df['dropoff_neighborhood'].isin(Midtown)))] 
        
    #From Midtown to Upper East Side 
    dfMidU = df[((df['dropoff_neighborhood'].isin(UpperEastSide))  
            & (df['pickup_neighborhood'].isin(Midtown)))] 
         
    #Within Upper East Side
    dfUU = df[((df['dropoff_neighborhood'].isin(UpperEastSide))  
            & (df['pickup_neighborhood'].isin(UpperEastSide)))]

We group together rides departing withing 15 minutes windows, with the same starting and ending destinations. I define a "week_minute" which is the number of minutes from the last Monday. This makes grouping by week and and arbitrary minute window easier.

We can add the passengers in LGA<->Upper Manhattan transit to Astoria, by assuming they'll be in Astoria after traveling for some while(20 minutes from LGA to Astoria, and 30 minutes from Upper Manhattan to Astoria).

From here we can estimate the number of cars needed if everyone carpooled instead. This calculated as:
    carpoolCount = math.floor(passenger_count/carpoolGoal)
With a minimum set of one carpool if people are present, as we are still obligated to pick up customers, even if it is inefficient. I peg 3 passengers as an estimate for our carpooling goal (taxis usually have ~1.6 mean passengers).

Now that our data is transformed, it's now easy to find various aggregated metadata (which is also saved to the dataframe), including:
- Total Taxi Count
- Total Carpool Count
- Car Reduction Ratio
- Mile Reduction (Total miles driven * Car Reduction Ratio)
- CO2 Reduction (Tons, 411 grams per mile as reported by the EPA)
- Total Fare
- Mean Passengers Per Taxi

Making Business Sense
--------------

While there will always be a market for single-rides, by calculating a theoretical all-carpool market, we can gauge the potential for Via service, and compare it to other known neighborhoods. The more cars that can be eliminated by carpooling, the more effective each carpool is. Thus I define 
- carpool potential = taxi_count - carpool_count
- carpool efficiency =  carpool_count/taxi_count
Why these two indicators? Carpooling efficiency is essentially the margin for carpooling; some routes have demand such that they are simply lend themselves to carpooling more readily. However, we shouldn't ignore the absolute number of rides that can be eliminated, which is our carpool potential. 

Caveats and Assumptions
--------------

There aspects of the data I had to assume, as there are many facets of Via's inner business model I do not know. The estimate above can be refined if I can gain more accurate data on the following:

I estimated a carpool goal of 3, but on average, how many people need to be riding for Via to make a profit? Is there a goal that includes rider comfort as well? The answers to these questions can help determine whether carpooling potential or efficiency are more important from a business standpoint.

Via's websites states an average wait time of 5 minutes for the user, and drivers are instructed to wait 1 minute. How does Via's routing algorithm work? What is the minutes per customer pickup efficiency in a given neighborhood? I used an estimate of 15 minutes for 3 people, but this could be further refined with real Via data. Clustering estimators and machine learning techniques could be used to aggregate people based on the pure lat/lon coordinates instead of simple time windows and neighborhoods, but at this point I suspect I would essentially be trying to recreate Via's own routing algorithm.

For the time it takes to get from LGA to Astoria, and Astoria to Upper Manhattan, a flat 20 minute and 30 minute transit time was used, respectively. This estimate could be further refined if historical traffic delay data was used, or if an accurate NYC traffic model could be utilized.

The nyc.gov is also missing out on a significant portion of real-world data: For-Hire Vehicle (FHV) rides. While nyc.gov does have data for FHV rides, they only state the pickup time, and occasionally pickup neighborhood (never drop-off neighborhood). Thus our current calculations are excluding services such as Uber, Lyft, and of course Via. So our total number of rides (and hence potential market) is larger than presented here, but we must also be wary of saturation and self-competition in neighborhoods where Via already has a presence.

In summary, my model gives an upper bound for carpooling efficiency from historical taxi data, which can be used as a barometer for Via service potential. With further information from Via such as business goals, extant Via-cle data, and routing models, I can further increase its accuracy. 
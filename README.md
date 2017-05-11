Astoria Carpooling Analysis
==============

Data Extraction
--------------
.csv files of taxi trips were downloaded from the following sources:
Pre 2015: http://www.andresmh.com/nyctaxitrips/
Post 2015: http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml 

Pandas was used for database management. Reading of the .csv files has to be done carefully, as the taxi data providers changed in 2015, thus presenting different formats. There are extraction scripts provided for each format.

Latitude and longitude for pickups/dropoffs were used to determine the corresponding neighborhood and borough. This was accomplished by reverse geocaching the lat/lon using the nyc.gov provided shapefile and the OGR python package.
Local reverse geocatching is used, as most (free) server-side reverse geocatching services will reject your IP after too many requests. Even performed locally, this process represents the most time-consuming process in the analysis.
*Note that starting July 2016, latitude and longitude are no-longer reported,* instead being replaced with a number corresponding to a neighborhood lookup table.

After reverse geocaching, to pare down file size we only save trips relevant to the Via challenge. We lump together nyc taxi zones as the following:
- Astoria = ['Astoria','Astoria Park']
- Midtown = ['Midtown Center','Midtown North','Midtown South','Midtown East']
- UpperEastSide = ['Upper East Side North','Upper East Side South']

Furthermore, we save LGA trips that begin/end in Astoria, and also if they have a Manhattan begining/end above a latitude of 40.76. This is due to the fact that Northerly Manhattan<->LGA trips require traversing either the Queensboro or Kennedy bridge, the entrances of which are near Astoria. That particular latitude was chosen as an estimate, using the routes chosen by Google Maps.
The final processed dataframe is saved as a pickled Pandas dataframe for analysis.

Data Analysis
--------------
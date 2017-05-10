#Culls a week of data (2016-01-03) between Astoria Park and the World Trade Center

import pandas as pd
import glob as glob
import os
import plotly.plotly as py
from plotly.graph_objs import *
import plotly.graph_objs as go
import plotly.tools as tls
import cufflinks as cf
import datetime

filePath = os.path.dirname(os.path.realpath(__file__))
weekStart = '2016-01-03 00:00:00'

def parseWeekMinutes(string):
    string = string.lstrip('(').rstrip(']').split(', ')
    return (float(string[0])+float(string[1]))/2*60

def plotWeek(df):
    
    data = [{
        'y':df.passenger_count,
        'x':df.datetime,
        'name':"Trip",
        'type':'bar',
        }] 
        
    layout = go.Layout(
        title='Rides from Astoria to the World Trade Center, 1st week of Jan.',
        xaxis=dict(
            tickangle=60,
            range=[to_unix_time(df.datetime.min()),to_unix_time(df.datetime.max())]
        ),
        yaxis=dict(
            title='Total Passengers',
            autorange=True
        ))
    fig = go.Figure(data=data, layout=layout)
    #py.plot(fig,filename='Weekly_Commutes') 
    
def to_unix_time(dt):
    epoch =  datetime.datetime.utcfromtimestamp(0)
    return (dt - epoch).total_seconds() * 1000    

if __name__ == '__main__':
    df = pd.read_pickle(filePath + '/../analyzedDataJan2016/AM')

    
    df = df[((df['dropoff_neighborhood'].isin(['World Trade Center'])) 
                & (df['pickup_neighborhood'].isin(['Astoria Park'])))]

    df = df.loc[weekStart]
    df['week_minutes'] = df.index.get_level_values('week_minutes') 
    df['week_seconds'] = df['week_minutes'].apply(parseWeekMinutes)
    halfInterval = df['week_minutes'][0]
    df['datetime'] = pd.to_datetime(weekStart)
    df['half_interval'] = 7.5*60

    df.datetime = df.datetime + df.week_seconds.values.astype('timedelta64[s]') 
    df.datetime = df.datetime + df.half_interval.values.astype('timedelta64[s]') 
    
    df.to_csv('SingleWeek.csv')
    
    plotWeek(df)
    

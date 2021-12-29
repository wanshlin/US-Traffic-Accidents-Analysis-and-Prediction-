from datetime import time
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import os

def main():
    # Get path right
    # path = os.path.dirname(__file__)
    # path = path.rstrip("visulization")
    # os.chdir(path)
    path = os.path.dirname(__file__)
    path = path.rstrip("visulization")
    os.chdir(path)
    
    client = MongoClient("mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net")
    db = client.CMPT732
    
    # 1. month
    months = list(db["monthCount"].find())
    months = pd.DataFrame(months)
    fig = px.bar(months, x='month(date)', y='count',text="count",  color_continuous_scale=px.colors.sequential.Tealgrn)
    fig.update_traces(textposition='outside')
    fig.write_image(r"./frontend/public/time_vis/projectMonth.jpg")
    #2.most hours

    hour = list(db["mosthour"].find())
    hour = pd.DataFrame(hour)
    fig = px.bar(hour, x='hour', y='count',text="count",  color_continuous_scale=px.colors.sequential.Tealgrn)
    
    fig.update_traces(textposition='outside')
    fig.write_image(r"./frontend/public/time_vis/hour.jpg")

    #3.week day
    hour = list(db["weekday"].find())
    hour = pd.DataFrame(hour)
    fig = px.bar(hour, x='day_of_Week', y='count',text="count",  color_continuous_scale=px.colors.sequential.Tealgrn)
    
    fig.update_traces(textposition='outside')
    fig.write_image(r"./frontend/public/time_vis/week_day.jpg")


    #4.north and south:
    n_s = list(db["north_south"].find())
    n_s= pd.DataFrame(n_s)
    
    month=n_s['month'].to_numpy()
    n=n_s['count_north'].to_numpy()
    s=n_s['count_south'].to_numpy()
    
    fig = go.Figure(data=[
    go.Bar(name='North', x=month,y=n),
    go.Bar(name='South', x=month, y=s)
    ])
    fig.update_layout(
    xaxis_title="Month",
    yaxis_title="Count")

    fig.update_layout(barmode='group')
    fig.write_image(r"./frontend/public/time_vis/south_north.jpg")

    #5.red vs blue:
    fig = go.Figure(data=[go.Table(header=dict(values=['State', 'population','count','avg_accident']),
                 cells=dict(values=[['Red States', 'Blue States'], [60383802, 87412298],[61729,239559],[0.001022,0.002741]]))
                     ])
    fig.show()

    #6.dangerous road:

    road = list(db["dangerous_road"].find())
    road = pd.DataFrame(road)
    fig = px.bar(road, x='street', y='count', color='count',text="count",  color_continuous_scale=px.colors.sequential.Tealgrn)

    fig.update_traces(textposition='outside')
    fig.write_image(r"./frontend/public/addr_vis/dange_road.jpg")
    
    
    
if __name__ == '__main__':
    main()
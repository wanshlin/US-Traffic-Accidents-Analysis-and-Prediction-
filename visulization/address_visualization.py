from datetime import time
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import os

def main():
    # Get path right
    path = os.path.dirname(__file__)
    path = path.rstrip("visulization")
    os.chdir(path)
    
    client = MongoClient("mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net")
    db = client.CMPT732
    
    # Get all of the data
    cityAvg = list(db["CityAvg"].find())
    cityAvg = pd.DataFrame(cityAvg)
    cityAvg = cityAvg.sort_values("AvgSeverity",ascending = False)

    cityCount = list(db["CityCount"].find())
    cityCount = pd.DataFrame(cityCount)
    cityCount = cityCount.sort_values("Count",ascending = False)

    sideSeverityCount = list(db["SideAndSeverityCount"].find())
    sideSeverityCount = pd.DataFrame(sideSeverityCount)
    
    timezone = list(db["Timezone"].find())
    timezone = pd.DataFrame(timezone)

    stateCount = list(db["StateCount"].find())
    stateCount = pd.DataFrame(stateCount)
    stateCount = stateCount.sort_values("Count",ascending = False)

    fig = px.bar(cityAvg, x='City', y='AvgSeverity', color='AvgSeverity',text="AvgSeverity",  color_continuous_scale=px.colors.sequential.Tealgrn)
    fig.update_layout(yaxis_range=[min(cityAvg['AvgSeverity']) * 0.9, max(cityAvg['AvgSeverity']) * 1.1])
    fig.update_traces(textposition='outside')
    fig.write_image(r"./frontend/public/addr_vis/cityAvg.jpg")

    fig = px.bar(cityCount, x='City', y='Count', color='AvgSeverity',text="Count",  color_continuous_scale=px.colors.sequential.Blugrn)
    fig.update_layout(yaxis_range=[min(cityCount['Count']) * 0.9, max(cityCount['Count']) * 1.1])
    fig.update_traces(textposition='outside')
    fig.write_image(r"./frontend/public/addr_vis/cityCount.jpg")

    left = sideSeverityCount.loc[sideSeverityCount['Side'] == 'L']
    right = sideSeverityCount.loc[sideSeverityCount['Side'] == 'R']
    left = left.sort_values('Severity')
    right = right.sort_values('Severity')
    fig = go.Figure(data=[
    go.Bar(name='Left Side', x=left['Severity'], y=left["Count"], text = left["Count"]),
    go.Bar(name='Right Side', x=right['Severity'], y=right["Count"], text = right["Count"])
    ])
    fig.update_traces(textposition='outside')
    fig.update_layout(barmode='group')
    fig.update_layout(
    xaxis_title="Severity",
    yaxis_title="Count",
    legend_title="Side")
    fig.write_image(r"./frontend/public/addr_vis/sideSeverityCount.jpg")
    
    fig = go.Figure(go.Pie(
    values = timezone['Count'],
    labels = timezone['Timezone'],
    text = timezone['AvgSeverity'],
    texttemplate = "%{value:,i}  %{percent} <br> Avg Severity: %{text}",
    textposition = "inside"))
    fig.write_image(r"./frontend/public/addr_vis/timezone.jpg")

    fig = px.bar(stateCount, x='State', y='Count', color='AvgSeverity',text="Count",  color_continuous_scale=px.colors.sequential.Blugrn)
    fig.update_layout(yaxis_range=[min(stateCount['Count']) * 0.9, max(stateCount['Count']) * 1.1])
    fig.update_traces(textposition='outside')
    fig.write_image(r"./frontend/public/addr_vis/stateCount.jpg")

    
if __name__ == '__main__':
    main()
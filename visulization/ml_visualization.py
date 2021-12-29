import plotly.offline as pyo
import plotly.graph_objects as go
import pandas as pd
import pymongo
from pymongo import MongoClient


def goBar(x, y, name):
    return go.Bar(x=x,
                  y=y,
                  name=name,
                  text=y,
                  textposition='outside')

def goGeo(df, labels, label_title):
    figure = go.Figure(
        data=go.Scattergeo(
            lon=df['Start_Lng'],
            lat=df['Start_Lat'],
            text=labels,
            mode='markers',
            marker_color=labels,
            marker=dict(
                size=4,
                colorbar_title=label_title)
    ))

    figure.update_layout(geo_scope='usa', width=800)
    return figure


def main():

    client = MongoClient('mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net')
    db = client.CMPT732
    
    result = pd.DataFrame(list(db['dtPrediction'].find()))
    sev1_result = pd.DataFrame(list(db['Sev1Prediction'].find()))
    sev2_result = pd.DataFrame(list(db['Sev2Prediction'].find()))
    sev3_result = pd.DataFrame(list(db['Sev3Prediction'].find()))

    fig1 = go.Figure()
    fig1.add_trace(
            goBar(sev1_result['Severity'],
                  sev1_result['count'],
                  'Predicted Severity 1'))
    fig1.add_trace(
            goBar(sev2_result['Severity'],
                  sev2_result['count'],
                  'Predicted Severity 2'))
    fig1.add_trace(
            goBar(sev3_result['Severity'],
                  sev3_result['count'],
                  'Predicted Severity 3'))

    fig1.update_layout(title_text='Prediceted Result of Decision Tree with Undersampling',
                      xaxis_title_text='Actual Severity Level',
                      yaxis_title_text='Count',
                      width=800)
    fig1.show()

    fig2 = goGeo(result, result['Severity'], 'Actual Severity')
    fig2.show()

    fig3 = goGeo(result, result['Severity_pr'], 'Predicted Severity')
    fig3.show()


if __name__ == '__main__':
    main()

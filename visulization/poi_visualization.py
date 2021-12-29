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


def figureBar(x, y, title, xlabel, ylabel, width, name=None):
    figure = go.Figure(data=[goBar(x,y,name)])
    figure.update_xaxes(type='category')
    figure.update_layout(
        title_text=title,
        xaxis_title_text=xlabel,
        yaxis_title_text=ylabel,
        width=width)

    return figure


def figurePie(labels, values, title, width):
    figure = go.Figure(
        data=[go.Pie(labels=labels, values=values)])
    figure.update_layout(title_text=title, width=width)
    return figure


def main():

    client = MongoClient('mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net')
    db = client.CMPT732
    avg_Visi_result = pd.DataFrame(list(db['AvgVisi'].find()))
    num_acc_result = pd.DataFrame(list(db['numPOI'].find()))
    severity_hasPOI_result = pd.DataFrame(list(db['SevHasPOI'].find()))
    severity_noPOI_result = pd.DataFrame(list(db['SevNoPOI'].find()))
    count_POI_result = pd.DataFrame(list(db['countPOI'].find()))
    count_severity_result = pd.DataFrame(list(db['countSev'].find()))

    fig1 = figureBar(avg_Visi_result['Severity'],
                     avg_Visi_result['avg(Visibility(mi))'],
                     'The Average Visibility of Severity',
                     'Severity Level',
                     'Average',
                     700)
    fig1.show()

    fig2 = figureBar(num_acc_result['has_POI'],
                     num_acc_result['count'],
                     'The Number of Accidents w/o POI',
                     'with POI',
                     'Count',
                     500)
    fig2.show()

    fig3 = figurePie(num_acc_result['has_POI'],
                     num_acc_result['count'],
                     'The Percentages of POI',
                     500)
    fig3.show()

    fig4 = go.Figure()
    fig4.add_trace(
        goBar(severity_hasPOI_result['Severity'],
              severity_hasPOI_result['count'],
              'has POI'))

    fig4.add_trace(
        goBar(severity_noPOI_result['Severity'],
              severity_noPOI_result['count'],
              'no POI'))

    fig4.update_layout(title_text='The Number of Different Severity Level Accidents w/o POI',
                       xaxis_title_text='Severity Level',
                       yaxis_title_text='Count',
                       width=800)

    fig4.show()

    POI = ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction',
           'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop',
           'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']

    fig5 = figureBar(POI,
                     count_POI_result.values.tolist()[0],
                     'The Number of Accidents with Different POI',
                     'POI',
                     'Count',
                     800)
    fig5.show()

    fig6 = figurePie(POI,
                     count_POI_result.values.tolist()[0],
                     'The Percentages of Accidents with Different POI',
                     600)
    fig6.show()

    fig7 = go.Figure()
    for c in POI:
        col = 'sum(' + c + ')'
        fig7.add_trace(
            goBar(count_severity_result['Severity'],
                  count_severity_result[col],
                  c))
    fig7.update_layout(title_text='The Bar Chart of Severity Level with POI',
                       xaxis_title_text='Severity Level',
                       yaxis_title_text='Count',
                       width=1000)
    fig7.show()


if __name__ == '__main__':
    main()

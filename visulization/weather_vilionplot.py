from pymongo import MongoClient
import plotly as py
import plotly.graph_objs as go
import pandas as pd
pyplt = py.offline.plot
from plotly.subplots import make_subplots

client = MongoClient("mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net")
db = client.CMPT732

df_weather_density=pd.DataFrame(list(db['WeatherDensity'].find()))


#........................Pressure(in), Visibility(mi), Wind_Speed(mph)..................................................
pvw = make_subplots(rows=1, cols=3,
                subplot_titles=("Pressure(in)", "Visibility(mi)", "Wind_Speed(mph)"))

#Pressure(in)
pre_3 =dict(type='violin',
    y=df_weather_density['Pressure(in)'][df_weather_density['Severity'] == 3],
    name=3,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(7,40,89)',
    line_color='rgb(7,40,89)')

pre_2 =dict(type='violin',
    y=df_weather_density['Pressure(in)'][df_weather_density['Severity'] == 2],
    name=2,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(8,81,156)',
    line_color='rgb(8,81,156)')

pre_1 =dict(type='violin',
    y=df_weather_density['Pressure(in)'][df_weather_density['Severity'] == 1],
    name=1,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(107,174,214)',
    line_color='rgb(107,174,214)')   
  
pvw.add_trace(pre_1, 1, 1)
pvw.append_trace(pre_2, 1, 1)
pvw.append_trace(pre_3, 1, 1)

#Visibility(mi)
vis_3 =dict(type='violin',
    y=df_weather_density['Visibility(mi)'][df_weather_density['Severity'] == 3],
    name=3,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(7,40,89)',
    showlegend=False,
    line_color='rgb(7,40,89)')

vis_2 =dict(type='violin',
    y=df_weather_density['Visibility(mi)'][df_weather_density['Severity'] == 2],
    name=2,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(8,81,156)',
    showlegend=False,
    line_color='rgb(8,81,156)')

vis_1 =dict(type='violin',
    y=df_weather_density['Visibility(mi)'][df_weather_density['Severity'] == 1],
    name=1,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(107,174,214)',
    showlegend=False,
    line_color='rgb(107,174,214)')     

pvw.add_trace(vis_1, 1, 2)
pvw.append_trace(vis_2, 1, 2)
pvw.append_trace(vis_3, 1, 2)

#Wind_Speed(mph)
win_3 =dict(type='violin',
    y=df_weather_density['Wind_Speed(mph)'][df_weather_density['Severity'] == 3],
    name=3,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(7,40,89)',
    showlegend=False,
    line_color='rgb(7,40,89)')

win_2 =dict(type='violin',
    y=df_weather_density['Wind_Speed(mph)'][df_weather_density['Severity'] == 2],
    name=2,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(8,81,156)',
    showlegend=False,
    line_color='rgb(8,81,156)')

win_1 =dict(type='violin',
    y=df_weather_density['Wind_Speed(mph)'][df_weather_density['Severity'] == 1],
    name=1,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(107,174,214)',
    showlegend=False,
    line_color='rgb(107,174,214)')

pvw.add_trace(win_1, 1, 3)
pvw.append_trace(win_2, 1, 3)
pvw.append_trace(win_3, 1, 3)

pvw.update_layout(legend_title_text='Severity',
                autosize=False,
                width=1000,
                height=800)
pvw = go.Figure(pvw)
pvw.show()
#pyplt(pvw,filename='weather_d_1.html',image='png')



#........................Temperature(F), Humidity(%)...................................................................
th = make_subplots(rows=1, cols=2,
                subplot_titles=("Temperature(F)", "Humidity(%)"))

#Temperature(F)
tem_3 =dict(type='violin',
    y=df_weather_density['Temperature(F)'][df_weather_density['Severity'] == 3],
    name=3,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(7,40,89)',
    line_color='rgb(7,40,89)')

tem_2 =dict(type='violin',
    y=df_weather_density['Temperature(F)'][df_weather_density['Severity'] == 2],
    name=2,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(8,81,156)',
    line_color='rgb(8,81,156)')

tem_1 =dict(type='violin',
    y=df_weather_density['Temperature(F)'][df_weather_density['Severity'] == 1],
    name=1,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(107,174,214)',
    line_color='rgb(107,174,214)')   

th.add_trace(tem_1, 1, 1)
th.append_trace(tem_2, 1, 1)
th.append_trace(tem_3, 1, 1)

#Humidity(%)
hum_3 =dict(type='violin',
    y=df_weather_density['Humidity(%)'][df_weather_density['Severity'] == 3],
    name=3,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(7,40,89)',
    showlegend=False,
    line_color='rgb(7,40,89)')

hum_2 =dict(type='violin',
    y=df_weather_density['Humidity(%)'][df_weather_density['Severity'] == 2],
    name=2,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(8,81,156)',
    showlegend=False,
    line_color='rgb(8,81,156)')

hum_1 =dict(type='violin',
    y=df_weather_density['Humidity(%)'][df_weather_density['Severity'] == 1],
    name=1,
    box_visible=True,
    meanline_visible=True,
    marker_color='rgb(107,174,214)',
    showlegend=False,
    line_color='rgb(107,174,214)')    

th.add_trace(hum_1, 1, 2)
th.append_trace(hum_2, 1, 2)
th.append_trace(hum_3, 1, 2)

th.update_layout(legend_title_text='Severity',
                autosize=False,
                width=1000,
                height=800)

th = go.Figure(th)
#pyplt(th,filename='weather_d_2.html',image='png')
th.show()






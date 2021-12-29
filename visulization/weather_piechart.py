from pymongo import MongoClient
import plotly as py
import plotly.graph_objs as go
import pandas as pd
pyplt = py.offline.plot
from plotly.subplots import make_subplots

client = MongoClient("mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net")
db = client.CMPT732

df_weather_condition=pd.DataFrame(list(db['WeatherCondition'].find()))
df_weather_wind=pd.DataFrame(list(db['WeatherWind'].find()))

#.......................Weather Condition..............................................................................
colors_condition = ['rgb(107,174,214)', 'rgb(8,81,156)', 'rgb(7,40,89)']
specs_condtion = [[{'type':'domain'}, {'type':'domain'}, {'type':'domain'}],
        [{'type':'domain'}, {'type':'domain'}, {'type':'domain'}]]

condition = make_subplots(rows=2, cols=3, specs=specs_condtion,
                    subplot_titles=("Clear & Cloudy", "Fog", "Rain", "Snow", "Storm", "Sand & Dust"))
labels_condition = ['1','2','3']

clear_1 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Clear & Cloudy'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Clear & Cloudy'])['Severity']==1])['Counts'])
clear_2 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Clear & Cloudy'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Clear & Cloudy'])['Severity']==2])['Counts'])
clear_3 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Clear & Cloudy'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Clear & Cloudy'])['Severity']==3])['Counts'])
list_clear = [clear_1,clear_2,clear_3]
condition.add_trace(go.Pie(labels=labels_condition,values=list_clear,marker={'colors':colors_condition}), 1, 1)

fog_1 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Fog'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Fog'])['Severity']==1])['Counts'])
fog_2 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Fog'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Fog'])['Severity']==2])['Counts'])
fog_3 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Fog'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Fog'])['Severity']==3])['Counts'])
list_fog = [fog_1,fog_2,fog_3]
condition.add_trace(go.Pie(labels=labels_condition,values=list_fog,marker={'colors':colors_condition}), 1, 2)

rain_1 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Rain'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Rain'])['Severity']==1])['Counts'])
rain_2 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Rain'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Rain'])['Severity']==2])['Counts'])
rain_3 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Rain'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Rain'])['Severity']==3])['Counts'])
list_rain = [rain_1,rain_2,rain_3]
condition.add_trace(go.Pie(labels=labels_condition,values=list_rain,marker={'colors':colors_condition}), 1, 3)

snow_1 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Snow'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Snow'])['Severity']==1])['Counts'])
snow_2 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Snow'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Snow'])['Severity']==2])['Counts'])
snow_3 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Snow'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Snow'])['Severity']==3])['Counts'])
list_snow = [snow_1,snow_2,snow_3]
condition.add_trace(go.Pie(labels=labels_condition,values=list_snow,marker={'colors':colors_condition}), 2, 1)

storm_1 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Storm'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Storm'])['Severity']==1])['Counts'])
storm_2 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Storm'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Storm'])['Severity']==2])['Counts'])
storm_3 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Storm'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Storm'])['Severity']==3])['Counts'])
list_storm = [storm_1,storm_2,storm_3]
condition.add_trace(go.Pie(labels=labels_condition,values=list_storm,marker={'colors':colors_condition}), 2, 2)

sand_1 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Sand & Dust'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Sand & Dust'])['Severity']==1])['Counts'])
sand_2 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Sand & Dust'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Sand & Dust'])['Severity']==2])['Counts'])
sand_3 =int(((df_weather_condition[df_weather_condition['Weather_Condition'] == 'Sand & Dust'])[(df_weather_condition[df_weather_condition['Weather_Condition'] == 'Sand & Dust'])['Severity']==3])['Counts'])
list_sand = [sand_1,sand_2,sand_3]
condition.add_trace(go.Pie(labels=labels_condition,values=list_sand,marker={'colors':colors_condition}), 2, 3)

condition.update_traces(hoverinfo='label+percent+name', textinfo='percent')
condition.update_layout(legend_title_text='Severity',
                autosize=False,
                width=1000,
                height=800)

condition = go.Figure(condition)
condition.show()
#pyplt(condition,filename='weather_c.html',image='png')



#.......................Wind Direction.................................................................................
colors_wind = ['rgb(107,174,214)','rgb(8,81,156)','rgb(7,40,89)' ]

specs_wind = [[{'type':'domain'}, {'type':'domain'}, {'type':'domain'}],
        [{'type':'domain'}, {'type':'domain'}, {'type':'domain'}]]

wind = make_subplots(rows=2, cols=3,specs = specs_wind,
                    subplot_titles=("Variable", "Clam", "South", "East", "North", "West"))
labels_wind = ['1','2','3']

variable_1 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'Variable'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'Variable'])['Severity']==1])['Counts'])
variable_2 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'Variable'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'Variable'])['Severity']==2])['Counts'])
variable_3 = int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'Variable'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'Variable'])['Severity']==3])['Counts'])
list_variable = [variable_1,variable_2,variable_3]
wind.add_trace(go.Pie(labels=labels_wind,values=list_variable,marker={'colors':colors_wind}), 1, 1)

clam_1 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'Clam'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'Clam'])['Severity']==1])['Counts'])
clam_2 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'Clam'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'Clam'])['Severity']==2])['Counts'])
clam_3 = int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'Clam'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'Clam'])['Severity']==3])['Counts'])
list_clam = [clam_1,clam_2,clam_3]
wind.add_trace(go.Pie(labels=labels_wind,values=list_clam,marker={'colors':colors_wind}), 1, 2)

south_1 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'South'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'South'])['Severity']==1])['Counts'])
south_2 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'South'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'South'])['Severity']==2])['Counts'])
south_3 = int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'South'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'South'])['Severity']==3])['Counts'])
list_s = [south_1,south_2,south_3]
wind.add_trace(go.Pie(labels=labels_wind,values=list_s,marker={'colors':colors_wind}), 1, 3)

east_1 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'East'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'East'])['Severity']==1])['Counts'])
east_2 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'East'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'East'])['Severity']==2])['Counts'])
east_3 = int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'East'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'East'])['Severity']==3])['Counts'])
list_e = [east_1,east_2,east_3]
wind.add_trace(go.Pie(labels=labels_wind,values=list_e,marker={'colors':colors_wind}), 2, 1)

north_1 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'North'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'North'])['Severity']==1])['Counts'])
north_2 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'North'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'North'])['Severity']==2])['Counts'])
north_3 = int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'North'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'North'])['Severity']==3])['Counts'])
list_n = [north_1,north_2,north_3]
wind.add_trace(go.Pie(labels=labels_wind,values=list_n,marker={'colors':colors_wind}), 2, 2)

west_1 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'West'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'West'])['Severity']==1])['Counts'])
west_2 =int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'West'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'West'])['Severity']==2])['Counts'])
west_3 = int(((df_weather_wind[df_weather_wind['Wind_Direction'] == 'West'])[(df_weather_wind[df_weather_wind['Wind_Direction'] == 'West'])['Severity']==3])['Counts'])
list_w = [west_1,west_2,west_3]
wind.add_trace(go.Pie(labels=labels_wind,values=list_w,marker={'colors':colors_wind}), 2, 3)

wind.update_traces(hoverinfo='label+percent+name', textinfo='percent')
wind.update_layout(legend_title_text='Severity',
                autosize=False,
                width=1000,
                height=800)

wind = go.Figure(wind)
#pyplt(wind,filename='weather_w.html',image='png')
wind.show()



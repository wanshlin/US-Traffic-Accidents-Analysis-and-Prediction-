from pymongo import MongoClient
import plotly as py
import plotly.graph_objs as go
import pandas as pd
pyplt = py.offline.plot

client = MongoClient("mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net")
db = client.CMPT732

df_weather_condition=pd.DataFrame(list(db['WeatherCondition'].find()))
df_weather_wind=pd.DataFrame(list(db['WeatherWind'].find()))



# Load the datasets from mongodb
df_pred_results = pd.DataFrame(list(db['PredResults_overunder'].find()))
pred_s1 = pd.DataFrame(list(db['PredS1_overunder'].find()))
pred_s2 = pd.DataFrame(list(db['PredS2_overunder'].find()))
pred_s3 = pd.DataFrame(list(db['PredS3_overunder'].find()))


#.......................Map of Prediction Severity......................................................................
map_pre = go.Figure(data=go.Scattergeo(
            lon=df_pred_results['Start_Lng'],
            lat=df_pred_results['Start_Lat'],
            text=df_pred_results['Pred_Severity'],
            mode='markers',
            marker_color=df_pred_results['Pred_Severity'],
            marker=dict(
                size=4,
                colorscale='viridis',
                colorbar_title='Predicted Severity')
    ))

map_pre.update_layout(geo_scope='usa', width=1000, height=800)  
#pyplt(map_pre,filename='ml_pre_over.html',image='png') 
map_pre.show()


#.......................Map of Actual Severity..........................................................................
map_actl = go.Figure(data=go.Scattergeo(
            lon=df_pred_results['Start_Lng'],
            lat=df_pred_results['Start_Lat'],
            text=df_pred_results['Severity'],
            mode='markers',
            marker_color=df_pred_results['Severity'],
            marker=dict(
                size=4,
                colorscale='viridis',
                colorbar_title='Actual Severity')
    ))

map_actl.update_layout(geo_scope='usa', width=1000, height=800)  
#pyplt(map_actl,filename='ml_actl_over.html',image='png')  
map_actl.show()



#.......................Count Prediction Severity.......................................................................
fig = go.Figure()
pre_1 =go.Bar(x=pred_s1['Severity'],
              y=pred_s1['count'],
              name='Predicted Severity 1',
              text=pred_s1['count'],
              textposition='outside')
fig.add_trace(pre_1)

pre_2 =go.Bar(x=pred_s2['Severity'],
              y=pred_s2['count'],
              name='Predicted Severity 2',
              text=pred_s2['count'],
              textposition='outside')
fig.add_trace(pre_2)          

pre_3 =go.Bar(x=pred_s3['Severity'],
              y=pred_s3['count'],
              name='Predicted Severity 3',
              text=pred_s3['count'],
              textposition='outside')
fig.add_trace(pre_3)                 

fig.update_layout(title_text='Prediceted Result of Decision Tree with Oversampling and Undersampling',
                    xaxis_title_text='Actual Severity Level',
                    yaxis_title_text='Count',
                    width=1000, 
                    height=800)
#pyplt(fig,filename='ml_count_over.html',image='png')                      
fig.show()


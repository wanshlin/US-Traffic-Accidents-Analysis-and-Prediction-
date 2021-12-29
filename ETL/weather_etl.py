import pyspark
import os
import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
from pyspark.sql import SparkSession  
from pyspark.sql.types import *
from pyspark.sql.functions import *
import _osx_support
spark = SparkSession\
        .builder\
        .master('local[2]')\
        .appName('accidents_etl')\
        .config("spark.mongodb.input.uri", 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.Project')\
        .config('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.Project')\
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
        .getOrCreate()

assert spark.version >= '3.0'  # make sure we have Spark 3.0+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

accidents_schema = StructType([
    StructField('ID', StringType()),
    StructField('Severity', IntegerType()),
    StructField('Start_Time', TimestampType()),
    StructField('End_Time', TimestampType()),
    StructField('Start_Lat', DoubleType()),
    StructField('Start_Lng', DoubleType()),
    StructField('End_Lat', DoubleType()),
    StructField('End_Lng', DoubleType()),
    StructField('Distance(mi)', DoubleType()),
    StructField('Description', StringType()),
    StructField('Number', StringType()),
    StructField('Street', StringType()),
    StructField('Side', StringType()),
    StructField('City', StringType()),
    StructField('County', StringType()),
    StructField('State', StringType()),
    StructField('Zipcode', StringType()),
    StructField('Country', StringType()),
    StructField('Timezone', StringType()),
    StructField('Airport_Code', StringType()),
    StructField('Weather_Timestamp', TimestampType()),
    StructField('Temperature(F)', DoubleType()),
    StructField('Wind_Chill(F)', DoubleType()),
    StructField('Humidity(%)', DoubleType()),
    StructField('Pressure(in)', DoubleType()),
    StructField('Visibility(mi)', DoubleType()),
    StructField('Wind_Direction', StringType()),
    StructField('Wind_Speed(mph)', DoubleType()),
    StructField('Precipitation(in)', DoubleType()),
    StructField('Weather_Condition', StringType()),
    StructField('Amenity', StringType()),
    StructField('Bump', StringType()),
    StructField('Crossing', StringType()),
    StructField('Give_Way', StringType()),
    StructField('Junction', StringType()),
    StructField('No_Exit', StringType()),
    StructField('Railway', StringType()),
    StructField('Roundabout', StringType()),
    StructField('Station', StringType()),
    StructField('Stop', StringType()),
    StructField('Traffic_Calming', StringType()),
    StructField('Traffic_Signal', StringType()),
    StructField('Turning_Loop', StringType()),
    StructField('Sunrise_Sunset', StringType()),
    StructField('Civil_Twilight', StringType()),
    StructField('Nautical_Twilight', StringType()),
    StructField('Astronomical_Twiligh', StringType()),
])
path = os.path.dirname(__file__)
path = path.rstrip("ETL")
os.chdir(path)

df_load = spark.read.csv('Accident_No_NA.csv', schema=accidents_schema,header=True)

df_load = df_load.withColumn('Year', year(df_load['Start_Time'])).withColumn('Severity', (df_load['Severity']-1))
df_load = df_load.filter(df_load['Year']!=2020)

selected_columns = ['Severity', 'Temperature(F)', 'Humidity(%)', 'Pressure(in)', 'Visibility(mi)', 'Wind_Speed(mph)', 'Weather_Condition', 'Wind_Direction']
df_load = df_load.select(*selected_columns)

df_filter = df_load.filter(df_load['Temperature(F)'] > -999.0)\
        .filter(df_load['Humidity(%)'] > -999.0)\
        .filter(df_load['Visibility(mi)'] != -999.0)\
        .filter(df_load['Pressure(in)'] != -999.0)\
        .filter(df_load['Wind_Speed(mph)'] != -999.0)\
        .filter(df_load['Wind_Direction'] != 'Wind_DirectionEmpty')\
        .filter(df_load['Weather_Condition'] != 'Weather_ConditionEmpty').cache()


df_weather_density = df_filter.select(df_filter['Severity'], df_filter['Temperature(F)'], df_filter['Humidity(%)'],
                                          df_filter['Pressure(in)'], df_filter['Visibility(mi)'],
                                          df_filter['Wind_Speed(mph)'])
df_weather_density.write.format('mongo').mode('overwrite').option('spark.mongodb.output.uri',
        'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.WeatherDensity').save()


df_weather_condition = df_filter.select(df_filter['Severity'], df_filter['Weather_Condition'])
df_weather_condition = df_weather_condition.withColumn('Weather_Condition', when(df_weather_condition['Weather_Condition'].rlike('Fog|Overcast|Haze|Mist|Smoke'), 'Fog')
                                                       .when(df_weather_condition['Weather_Condition'].rlike('Clear|Fair|Cloudy|Clouds|Cloud'), 'Clear & Cloudy')
                                                       .when(df_weather_condition['Weather_Condition'].rlike('Rain|Showers|Drizzle|Thunder'), 'Rain')
                                                       .when(df_weather_condition['Weather_Condition'].rlike('Ice|Snow|Sleet|Hail|Wintry'), 'Snow')
                                                       .when(df_weather_condition['Weather_Condition'].rlike('Storm|storm|Tornado|Squalls'), 'Storm')
                                                       .when(df_weather_condition['Weather_Condition'].rlike('Stand|Dust'), 'Sand & Dust')
                                                       .otherwise(df_weather_condition['Weather_Condition']))
df_weather_condition = df_weather_condition.groupBy(df_weather_condition['Weather_Condition'],
                                            df_weather_condition['Severity']).count().withColumnRenamed('count', 'Counts')
df_weather_condition.write.format('mongo').mode('overwrite').option('spark.mongodb.output.uri',
        'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.WeatherCondition').save()


df_weather_wind = df_filter.select(df_filter['Severity'], df_filter['Wind_Direction'])
df_weather_wind = df_weather_wind.withColumn('Wind_Direction', when((df_weather_wind['Wind_Direction'] == 'WSW') | (df_weather_wind['Wind_Direction'] == 'WNW') | (df_weather_wind['Wind_Direction'] == 'W'), 'West')
                                                    .when((df_weather_wind['Wind_Direction'] == 'SSW') | (df_weather_wind['Wind_Direction'] == 'SSE') | (df_weather_wind['Wind_Direction'] == 'SW') | (df_weather_wind['Wind_Direction'] == 'S') | (df_weather_wind['Wind_Direction'] == 'SE'), 'South')
                                                    .when((df_weather_wind['Wind_Direction'] == 'NNW') | (df_weather_wind['Wind_Direction'] == 'NNE') | (df_weather_wind['Wind_Direction'] == 'NW') | (df_weather_wind['Wind_Direction'] == 'NE') | (df_weather_wind['Wind_Direction'] == 'N'), 'North')
                                                    .when((df_weather_wind['Wind_Direction'] == 'ESE') | (df_weather_wind['Wind_Direction'] == 'ENE') | (df_weather_wind['Wind_Direction'] == 'E'), 'East')
                                                    .when(df_weather_wind['Wind_Direction'] == 'CALM', 'Clam')
                                                    .when(df_weather_wind['Wind_Direction'] == 'VAR', 'Variable')
                                                    .otherwise(df_weather_wind['Wind_Direction']))
df_weather_wind = df_weather_wind.groupBy(df_weather_wind['Wind_Direction'], df_weather_wind['Severity']).count().withColumnRenamed('count', 'Counts')
df_weather_wind.write.format('mongo').mode('overwrite').option('spark.mongodb.output.uri',
        'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.WeatherWind').save()


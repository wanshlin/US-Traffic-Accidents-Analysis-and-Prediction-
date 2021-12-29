import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os

# Configure spark session
# ?readPreference=primaryPreferred
spark = SparkSession\
    .builder\
    .master('local[2]')\
    .appName('accidents_etl')\
    .config("spark.mongodb.input.uri", 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.Project')\
    .config('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.Project')\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
    .getOrCreate()

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
    StructField('Number', DoubleType()),
    StructField('Street', StringType()),
    StructField('Side', StringType()),
    StructField('City', StringType()),
    StructField('County', StringType()),
    StructField('State', StringType()),
    StructField('Zipcode', StringType()),
    StructField('Country', StringType()),
    StructField('Timezone', StringType()),
    StructField('Airport_Code', StringType()),
    StructField('Weather_Timestamp', StringType()),
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
    StructField('Astronomical_Twilight', StringType()),
])

# Change the current working directory to root
path = os.path.dirname(__file__)
path = path.rstrip("/ETL")
os.chdir(path)

# Load df
df = spark.read.csv("Accident_No_NA.csv", schema=accidents_schema,header=True)

df.select(df['Start_Time'])


df=df.withColumn('date',to_date(df['Start_Time'],"yyyy-MM-dd"))             #convert timestamp to datetime

df=df.select(df['State'],df['start_lat'],df['date'],year(df['date']).alias('Year'), month(df['date']).alias('Month'),dayofmonth(df['date']),df['Timezone']).cache()

df=df.filter((df['Year']=='2017')|(df['Year']=='2018')|(df['Year']=='2019')).cache()

#1 month:
df1=df.groupBy(df['Month']).count().orderBy(df['Month'])
df1.show()
df1.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.monthCount').save()

#2. weekday:

df2 = df.select(dayofweek(df['date']).alias('day_of_Week'))
df2 = df2.groupBy(df2['day_of_Week']).count().orderBy(df2['day_of_Week'])
df2.show()

    #df2=df1.filter(df1['year(date)']=='2020')
    #df2=df1.filter(df1['state_name']=='FL') 
    #df2=df2.filter(df1['start_lat']>45)  

df2.write.format('mongo')\
   .mode('overwrite')\
   .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.dayofWeek').save()

#3.north vs south:

df3=df.filter(df['start_lat']>37)
df4=df.filter(df['start_lat']<30)
df3=df3.filter(df3['Timezone']=='US/Eastern')
df4=df4.filter(df4['Timezone']=='US/Eastern')
df3=df3.groupBy(df3['Month']).count().orderBy(df3['Month'])
df4=df4.groupBy(df4['Month']).count().orderBy(df4['Month'])
df3.show()
df4.show()

df3.write.format('mongo')\
     .mode('overwrite')\
     .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.northCount').save()
df4.write.format('mongo')\
     .mode('overwrite')\
     .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.southCount').save()
 













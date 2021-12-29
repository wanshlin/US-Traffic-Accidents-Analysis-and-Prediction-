import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
from pyspark.sql import functions

def dataFiltering(data):
    newdata = data.filter(functions.year(data['Start_Time'])<2020)
    newdata = newdata.withColumn('Severity', newdata['Severity']-1)
    return newdata    

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
path = path.rstrip("ETL")
os.chdir(path)

# Load df
df = spark.read.csv(r"./Accident_No_NA.csv", schema=accidents_schema,header=True)
population = spark.read.format("mongo").option("uri","mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.population").load()
df = df.cache()
addressColumns = ['Distance(mi)','Severity','Street','Side','City','County','State','Zipcode','Timezone']
# Use stared expresison to unpack a list
df = dataFiltering(df)
df = df.select(*addressColumns)

# ======================VISUALIZATION======================
# Side and Severity barplot
graphDf = df.groupBy("Side","Severity").count().alias("Count").orderBy("Severity","Side")
graphDf = graphDf.withColumnRenamed("count","Count")
graphDf = graphDf.cache()
# graphDf = graphDf.withColumn("GraphCategory", lit("SideAndSeverityCount"))

graphDf.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.SideAndSeverityCount').save()


# Top 10 City Accident Count
graphDf = df.groupBy("City")\
    .agg(count("Severity").alias("Count")\
    ,avg("Severity").alias("Avg"))\
    .orderBy(col("Count").desc()).limit(10)
graphDf = graphDf.cache()
graphDf = graphDf.select("*", round(col('Avg'),2).alias("AvgSeverity")).drop("Avg")


graphDf.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.CityCount').save()


# Top 10 State Accident Count
graphDf = df.groupBy("State")\
    .agg(count("Severity").alias("Count")\
    ,avg("Severity").alias("Avg"))\
    .orderBy(col("Count").desc()).limit(10)
graphDf = graphDf.cache()
graphDf = graphDf.select("*", round(col('Avg'),2).alias("AvgSeverity")).drop("Avg")
population = population.withColumnRenamed("state","populationState")
graphDf = graphDf.join(population,graphDf.State ==  population.populationState,"inner")
graphDf = graphDf.select("State","Count","AvgSeverity","pop")
graphDf = graphDf.withColumn('AccidentsPerPerson', graphDf['Count']/graphDf['pop'])


graphDf.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.StateCount').save()


# Top 10 City vs AvgSeverity
graphDf = df.groupBy("City")\
    .agg(count("Severity").alias("Count")\
    ,avg("Severity").alias("Avg"))\
    .orderBy(col("Avg").desc()).where(col("Count") > 100).limit(10)
graphDf = graphDf.select("*", round(col('Avg'),3).alias("AvgSeverity")).drop("Avg")
graphDf = graphDf.cache()
# graphDf = graphDf.withColumn("GraphCategory", lit("CityAvg"))
# graphDf.show(100,False)

graphDf.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.CityAvg').save()




# Timezone difference
graphDf = df.groupBy("Timezone")\
    .agg(count("Severity").alias("Count")\
    ,avg("Severity").alias("Avg"))\
    .orderBy(col("Count").desc()).limit(10)
graphDf = graphDf.select("*", round(col('Avg'),3).alias("AvgSeverity")).drop("Avg")
# graphDf = graphDf.withColumn("GraphCategory", lit("Timezone"))

graphDf.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.Timezone').save()




# TIME
df = spark.read.csv('Accident_No_NA.csv',schema=accidents_schema, header=True)
df.select(df['Start_Time'])
    
df1=df.withColumn('date',to_date(df['Start_Time'],"yyyy-MM-dd"))             #convert timestamp to datetime

df1=df1.select(df1['State'],df1['start_lat'],df1['date'],year(df1['date']),month(df1['date']),dayofmonth(df1['date'])).cache()

#1. years:

df2=df1.filter((df1['year(date)']=='2017')|(df1['year(date)']=='2018')|(df1['year(date)']=='2019'))
#df2=df1.filter(df1['year(date)']=='2020')
#df2=df1.filter(df1['state_name']=='FL') 
#df2=df2.filter(df1['start_lat']>45)          
df2=df2.groupBy(df2['month(date)']).count().orderBy(df2['month(date)'])
df2.write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.monthCount').save()

# Add state
# Aggregate: Side of road and severity
# Top 5 city








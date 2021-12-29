import sys
import uuid
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
import re
import math
from datetime import datetime
from pyspark.sql import SparkSession, functions, types
#from cassandra.cluster import Cluster
from pyspark.sql.functions import *
#from pyspark.sql.types import StructType
#from pyspark.sql.types import StructField
from pyspark.sql.types import *
import os



#def main(keyspace, table):
def main():
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
        StructField('Start_Lat', FloatType()),
        StructField('Start_Lng', FloatType()),
        StructField('End_Lat', FloatType()),
        StructField('End_Lng', FloatType()),
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

    path = os.path.dirname(__file__)
    path = path.rstrip("ETL")
    os.chdir(path)
    
    #df = spark.read.format("org.apache.spark.sql.cassandra").options(table=table, keyspace=keyspace).load()
    df = spark.read.csv('Accident_No_NA.csv',schema=accidents_schema, header=True)
    df.select(df['Start_Time'])
    df_h=df.select(hour(df['Start_Time']).alias('hour'))
    df=df.withColumn('date',to_date(df['Start_Time'],"yyyy-MM-dd HH:mm:ss"))             #convert timestamp to datetime

    df=df.select(df['State'],df['start_lat'],df['date'],year(df['date']).alias('Year'), month(df['date']).alias('Month'),dayofmonth(df['date']),df['Timezone'],df['city'],df['Weather_Condition']).cache()

    df=df.filter((df['Year']=='2017')|(df['Year']=='2018')|(df['Year']=='2019')).cache()    
    

    #1. years:

    df2=df.groupBy(df['Month']).count().orderBy(df['Month'])
    df2.repartition(1).write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.mostMonth').save()


    #2. weekday:

    df_w = df.select(dayofweek(df['date']).alias('day_of_Week'))
    df_w = df_w.groupBy(df_w['day_of_Week']).count().orderBy(df_w['day_of_Week'])
    df_w.repartition(1).write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.weekday').save()
    
    #3.south vs north
    df3=df.filter(df['start_lat']>40)
    df4=df.filter(df['start_lat']<35)
    df3=df3.filter(df3['Timezone']=='US/Eastern')
    df4=df4.filter(df4['Timezone']=='US/Eastern')
    df3=df3.select(df3['Month'].alias('month'))
    df3=df3.groupBy(df3['Month']).count().withColumnRenamed('count','count_north').orderBy(df3['Month'])
    df4=df4.groupBy(df4['Month']).count().withColumnRenamed('count','count_south').orderBy(df4['Month'])
    df3=df3.join(df4,df3['month']==df4['Month']).select(df3['month'],df3['count_north'],df4['count_south']).orderBy('month')

    df3.repartition(1).write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.north_south').save()
    
    
    #4. hours count:
    df5 = df_h.groupBy(df_h['hour']).count().orderBy(df_h['hour'])
    df5.repartition(1).write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.mostHour').save()


    #5.most day of top 10 city:
    df6 = df.groupBy(df['city']).count()
    df6=df6.sort(df6['count'],ascending=False).limit(10)
    df6=df6.select(df6['city'].alias('city1'),df6['count'])
    join_df6= df6.join(df,df['city']==df6['city1']).cache()
    df_6=join_df6.groupBy(join_df6['date'],join_df6['city']).count().orderBy('count',ascending=False).limit(12)  #most accident day of top 10 city

    df_6.repartition(1).write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.most_day_city').save()

    df_6a=df_6.select(df_6['date'].alias('Date'),df_6['city'].alias('City'),df_6['count'].alias('Count'))
    df_6a=df_6a.join(df, (df_6a['Date']==df['date']) & (df_6a['City']== df['city'])).select(df_6a['Date'],df_6a['City'],df['Weather_Condition'],df_6a['Count']) #most accident day of top 10 city with weather
    
    df_6a.repartition(1).write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.most_day_city_weather').save()


    df_6b = df_6a.filter((df_6a['Date']=='2019-12-23') & (df_6a['City']=='Los Angeles'))
    df_6b=df_6b.groupBy(df_6b['Weather_Condition']).count().orderBy('count',ascending=False)
    df_6b.repartition(1).write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.la_1223_weather').save()

    #6.election
    pop_schema = StructType([
        StructField('state', StringType()),
        StructField('pop', IntegerType()),
        
    ])
    edf = spark.read.csv('election.csv', header=True)
    pdf = spark.read.csv('population.csv', schema=pop_schema,header=True)
    edf1=edf.join(df,df['State']==edf['state']).groupBy(edf['state'],edf['color']).count().groupBy('color').sum()
   
    pdf=pdf.join(edf,pdf['State']==edf['state']).groupBy('color').sum('pop')
    pdf=pdf.select(pdf['color'].alias('Color'),pdf['sum(pop)'])
    sdf=pdf.join(edf1,edf1['color']==pdf['color']).select(edf1['color'],pdf['sum(pop)'],edf1['sum(count)'])
    sdf=sdf.withColumn('avg_acc',round(sdf['sum(count)']/sdf['sum(pop)'],6))
    sdf.repartition(1).write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.red_blue').save()
    #7.street
    df7=df.groupBy(df['street']).count().orderBy('count',ascending=False).limit(10)
    df7.repartition(1).write.format('mongo')\
    .mode('overwrite')\
    .option('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.mostDangeRoad').save()
    df7.show()


if __name__ == '__main__':
    #keyspace = sys.argv[1]
    #table = sys.argv[2]
    #cluster_seeds = ['node1.local']
    spark = SparkSession\
    .builder\
    .master('local[2]')\
    .appName('accidents_etl')\
    .config("spark.mongodb.input.uri", 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.Project')\
    .config('spark.mongodb.output.uri', 'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.Project')\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
    .getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main()
    #spark = SparkSession.builder.appName('Spark Cassandra example').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
     # jwa378 - accident
    #assert spark.version >= '3.0' # make sure we have Spark 3.0+
    #spark.sparkContext.setLogLevel('WARN')
    #sc = spark.sparkContext
    #main(keyspace, table)
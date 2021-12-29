import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions
from pyspark.sql.types import *
from data_filtering import *


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
    StructField('Amenity', BooleanType()),
    StructField('Bump', BooleanType()),
    StructField('Crossing', BooleanType()),
    StructField('Give_Way', BooleanType()),
    StructField('Junction', BooleanType()),
    StructField('No_Exit', BooleanType()),
    StructField('Railway', BooleanType()),
    StructField('Roundabout', BooleanType()),
    StructField('Station', BooleanType()),
    StructField('Stop', BooleanType()),
    StructField('Traffic_Calming', BooleanType()),
    StructField('Traffic_Signal', BooleanType()),
    StructField('Turning_Loop', BooleanType()),
    StructField('Sunrise_Sunset', StringType()),
    StructField('Civil_Twilight', StringType()),
    StructField('Nautical_Twilight', StringType()),
    StructField('Astronomical_Twiligh', StringType()),
])

def main():

    data = spark.read.csv('Accident_No_NA.csv',
        schema=accidents_schema, header=True)

    data = dataFiltering(data)
    
    columns = ['ID', 'Severity', 'Visibility(mi)', 'Amenity', 'Bump',
            'Crossing', 'Give_Way', 'Junction', 'No_Exit', 'Railway',
            'Roundabout', 'Station', 'Stop', 'Traffic_Calming',
            'Traffic_Signal', 'Turning_Loop']

    POI_df = data.select(columns)
    POI_df = POI_df.withColumn('has_POI',
        functions.when((
            POI_df['Amenity'] | POI_df['Bump'] |
            POI_df['Crossing'] | POI_df['Give_Way'] |
            POI_df['Junction'] | POI_df['No_Exit'] |
            POI_df['Railway'] | POI_df['Roundabout'] |
            POI_df['Station'] | POI_df['Stop'] |
            POI_df['Traffic_Calming'] |
            POI_df['Traffic_Signal'] |
            POI_df['Turning_Loop']), True)
        .otherwise(False)
        ).cache()

    avg_visi = POI_df.filter(POI_df['Visibility(mi)']>=0.0)
    avg_visi = avg_visi.select('Severity', 'Visibility(mi)')
    avg_visi = avg_visi.groupBy('Severity').avg('Visibility(mi)')
    avg_visi = avg_visi.orderBy('Severity')

    avg_visi.write.format('mongo').mode('overwrite').option(
        'spark.mongodb.output.uri',
        'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.AvgVisi'
        ).save()
    
    num_acc = POI_df.groupby('has_POI').count()
    num_acc.write.format('mongo').mode('overwrite').option(
        'spark.mongodb.output.uri',
        'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.numPOI'
        ).save()

    severity_hasPOI = POI_df.filter(POI_df['has_POI'])
    severity_hasPOI = severity_hasPOI.select('Severity')
    severity_hasPOI = severity_hasPOI.groupby('Severity').count()
    severity_hasPOI = severity_hasPOI.orderBy('Severity')
    severity_hasPOI.write.format('mongo').mode('overwrite').option(
        'spark.mongodb.output.uri',
        'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.SevHasPOI'
        ).save()


    severity_noPOI = POI_df.filter(~POI_df['has_POI'])
    severity_noPOI = severity_noPOI.select('Severity')
    severity_noPOI = severity_noPOI.groupby('Severity').count()
    severity_noPOI = severity_noPOI.orderBy('Severity')
    severity_noPOI.write.format('mongo').mode('overwrite').option(
        'spark.mongodb.output.uri',
        'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.SevNoPOI'
        ).save()

    POI = ['Amenity', 'Bump', 'Crossing', 'Give_Way', 'Junction',
           'No_Exit', 'Railway', 'Roundabout', 'Station', 'Stop',
           'Traffic_Calming', 'Traffic_Signal', 'Turning_Loop']
    
    severity_POI = POI_df.filter(POI_df['has_POI'])
    severity_POI = severity_POI.select(
        [POI_df[c].cast(IntegerType())
         if c in POI else POI_df[c]
         for c in POI_df.columns]
         ).cache()

    count_POI = severity_POI.select(POI)
    count_POI = count_POI.groupby().sum()
    count_POI.write.format('mongo').mode('overwrite').option(
        'spark.mongodb.output.uri',
        'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.countPOI'
        ).save()

    count_severity = severity_POI.select(['Severity'] + POI)
    count_severity = count_severity.groupby('Severity').sum()
    count_severity = count_severity.orderBy('Severity')
    count_severity.write.format('mongo').mode('overwrite').option(
        'spark.mongodb.output.uri',
        'mongodb+srv://dbAdmin:cmpt732@cluster732.jfbfw.mongodb.net/CMPT732.countSev'
        ).save()


if __name__ == '__main__':


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
    main()

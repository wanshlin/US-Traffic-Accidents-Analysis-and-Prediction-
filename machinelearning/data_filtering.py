from pyspark.sql import functions
# This is a helper function to help transform the data

def dataFiltering(data):
    newdata = data.filter(functions.year(data['Start_Time'])<2020)
    newdata = newdata.withColumn('Severity', newdata['Severity']-1)
    return newdata
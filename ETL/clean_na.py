import pandas as pd
import datetime
from pyspark.sql import functions

def dataFiltering(data):
    newdata = data.filter(functions.year(data['Start_Time'])<2020)
    newdata = newdata.withColumn('Severity', newdata['Severity']-1)
    return newdata    

def main():
    df = pd.read_csv(r"./US_Accidents_Dec20_updated.csv")
    earliest_date = datetime.datetime.min
    # Check which columns have empty values
    nan_values = df.isna()
    nan_columns = nan_values.any()
    columns_with_nan = df.columns[nan_columns].tolist()
    print("Here is a list of columns containing empty")
    print(columns_with_nan)
    # Put street number as -999.0 for all missing street number
    # CityEmpty for all missing city and so on    
    values = {"Number": -999.0, 'City': "CityEmpty", "Zipcode" : "ZipCodeEmpty",
    'Timezone':"TimezoneEmpty",
    'Airport_Code':"Airport_CodeEmpty",
    'Weather_Timestamp': earliest_date,
    'Temperature(F)': -999.0,
    'Wind_Chill(F)': -999.0,
    'Humidity(%)': -999.0,
    'Pressure(in)': -999.0,
    'Visibility(mi)':-999.0,
    'Wind_Direction':"Wind_DirectionEmpty",
    'Wind_Speed(mph)':-999.0,
    'Precipitation(in)':-999.0,
    "Weather_Condition":"Weather_ConditionEmpty",
    'Sunrise_Sunset':"Sunrise_SunsetEmpty",
    'Civil_Twilight' : "Civil_TwilightEmpty", 
    'Nautical_Twilight' : "Nautical_TwilightEmpty", 
    'Astronomical_Twilight' : "Astronomical_TwilightEmpty"
    }
    df = df.fillna(value=values)
    # You can use this to check if there is any column containig na
    nan_values = df.isna()
    nan_columns = nan_values.any()
    columns_with_nan = df.columns[nan_columns].tolist()
    print("Here is a list of columns containing empty now:")
    print(columns_with_nan)
    # There is also this strange '\' symbol in description that will mess up 16 rows when we  import
    df['Description'].replace('[\\\\]','',inplace=True,regex=True)
    # Save
    df.to_csv(r"./Accident_No_NA.csv", index=False)

if __name__ == '__main__':
    main()

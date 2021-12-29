Our dataset is about US Accident, The main datasets we use is US Accidents (2016 - 2020), which is offered as a CSV file from Kaggle(https://www.kaggle.com/sobhanmoosavi/us-accidents). The dataset covers traffic accidents in 49 states(excluding Alaska) of the US with 47 distinctive columns and we hope to predict severity based on the data. In the time_etl.py file we also use two datasets: population.csv and election.csv, which can be downloaded directly from the Repository.

Here is a brief overview of the structure of the files:

**ETL folder:**
All the files for ETL purpose, prepared the aggregated data for later visulizations, which are stored in MongoDB cluster.
clean_na.py is a python script using panadas to replace NA values from the original dataset, please run this file prior to other python script because it will clean all the missing value data file needed by other scripts.

**visulization folder:**
All the files for visulizations, which will be needed by the front end system, partitioned by different features(for example address, weather, time, poi etc.).

**frontend folder:**
a working react webapp created by Next.js

**machinelearing folder:**
stored the codes related to machine learning model as well as models we produced. Please note that the model "OverUnderSamplePipelineModel" is our final model.

**Contributors:**
Shilin Wang 
Jiahe Wang 
Huiyi Zou 
Zhi Zheng 

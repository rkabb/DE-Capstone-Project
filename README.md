# DE-Capstone-Project

## Overview of project
This project will use the data provided by Udacity. The end use case would be have a source-of-truth database 
and can be used by business or analytics team to understand the immigration trend for better planning.

**Examples of data exploration scenarios/queries would be:**

-	Yearly/Quarterly/Monthly visitor arrival count based on Country Of Residence
    
     Ex: Top 10 visitor country

-	Yearly/Quarterly/Monthly arrival count based on Port Of Entry
    
     Ex: Top 10 ports

-	End user can also explore the data to find out the trend. What is the number of visitors 
    this year compared to last year? What is the monthly trend? What is the percentage variance ? 

-	We can also explore the city demography aggregated at state level.

##Data Source
**The data set used are:**

- Data on immigration to the United States . In this project, I have uploaded data for only two months of 2016.
We can upload data for all past years to study immigration trend.
source:https://travel.trade.gov/research/reports/i94/historical/2016.html

- airport codes
Source: https://datahub.io/core/airport-codes#data

- U.S. city demographics
Source:https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/

- World Temperature Data:   
Source:https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data


- I94_SAS_Labels_Descriptions.SAS
This file provides the field details and valid/invalid codes. This can be used to filter out the records.


## Data analysis
- US city data provide is already in a FACT table state. Some metrics are null for some cities. For ex,  number of veterans is NULL for Puerto Rico.  NULL values will be converted to zero.                                                                                                                       Race information is not provided by count. We will aggregate that data and pivot it to support the report/analytics at state level.

- Immigration data: I94_SAS_Labels_Descriptions.SAS gives the details of fields and valid & invalid codes. By using the data provided in this SAS file, new CSV files are created for State_Names, Ports & immigrant Origin country.
       These files will be used during joins to filter out invalid data.

- Additional CSV files created are:
    1.	ports.csv -- > contains Port code , Port City
    2.	state_names.csv -- > contains state code , state City
    3.	origin_country.csv  -- > contains country code, country name

## Technical Design
Immigration data volume is huge. We can have as many years of history data as we want and data for each month would contain 4 to 5 million.

All the source files are copied to HDFS. This helps to store the data irrespective of data size.  SPARK is used to process the files because of it’s high speed performance.
There is a EMR cluster set up on AWS.  

Suppose, if the data volume increases in size say by 100 times, we can just increase the number of cores in EMR to support large data set.
Immigration Data is expected to get updated once a month. And etl process will run once a month.

SPARK reada from HDFS ,  do data wrangling and finally store the required information in parquet format in S3.
From S3, anyone can read these files to further explore and analysis.
Immigration data is partitioned at Year, Month level.

Etl.py is a module which does the data movement.  There is a EMR cluster set up on AWS. Etl.py will run on this cluster.

We can set up data pipeline using airflow. This would help us to run the ETL pipeline on a regular schedule , say, once a month or once a day.
We can also add, data validation tasks in airflow DAG to ensure data load validity.

Currently, with this design, data is stored inS3 in parquet format and it should support fair amount of users.
If the user base increased, then we can store the data in DWH, such as RedShift to support more users.


## Conceptual Data model
        
Immigration data is modeled into 3 separate tables to support analysis at  different level. I choose to create separate table for different level, 
because that helps to generate reports and see the trend by comparing values with previous years.
City data is aggregated at state level. By aggregating the data at the state level, this table can be joined with Visitors_By_State to get more details for state.
For ex, if we want to know more details about the top 10 immigrant states, these two tables can be joined

- Visitors_per_country:

         |-- county: string 
         |-- visitors: long
         |-- year: integer 
         |-- month: integer 

- Visitors_per_ports:


         |-- port: string  
         |-- visitors: long  
         |-- year: integer  
         |-- month: integer  


- Visitors_per_state: 

         |-- state: string 
         |-- visitors: long 
         |-- year: integer 
         |-- month: integer


- Demography_By_State:

         |-- state: string 
         |-- code: string 
         |-- age: string 
         |-- Males: string 
         |-- females: string 
         |-- tot_Pop: string 
         |-- Veterans: string 
         |-- Foreign: string
         |-- Household: string 
         |-- Hispanic: string 
         |-- Asian: string 
         |-- White: string 
         |-- Black: string 
         |-- Native: string 

 ## Files included
 
 - etl.py - pipeline to read data from HDFS, process and store data in S3 in parquet format

 Notebooks shows how the data is validated after the load and explored to get reports:
 
     - Analyze Visitors By Country.ipynb
     - Analyze Visitors By Port.ipynb
     - Analyze Visitors By Visiting State.ipynb

 
 
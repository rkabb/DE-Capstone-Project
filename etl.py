#import configparser
from datetime import datetime
#import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col ,monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format ,from_unixtime


#config = configparser.ConfigParser()
#config.read('dl.cfg')

#os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
#os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    This function will create a spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0" ) \
        .enableHiveSupport()\
        .getOrCreate()
    return spark


def process_city_data(spark,   output_data):
    """
    This function will take two parameters:
    spark: spark session
    output_data: S3 bucket where analyzed data would be stored

    This will process U.S. City Demographic Data
    Creates two dimension tables: SONGS and ARTISTS.
    Files are written in parquet format
    """


    # read city data file
    city_data = spark.read.format("csv").option("header", "true").option("delimiter", ";").load(
        "us-cities-demographics.csv")

    # extract columns to create songs table
    city_data.createOrReplaceTempView("city")

    # get city avg data
    city_sum = spark.sql("""select state, `State Code` code ,format_number(avg(`Median Age`) ,'##') age , 
                               format_number(sum(`Male Population`),'##,###,###.##') Males , 
                               format_number(sum(`Female Population`),'##,###,###.##') females, 
                               format_number(sum(`Total Population`),'##,###,###.##') tot_Pop,
                               format_number(sum(`Number of Veterans`),'##,###,###.##') Veterans, 
                               format_number(sum(`Foreign-born`),'##,###,###.##') Foreign, 
                               format_number(avg(`Average Household Size` ),'##,###,###.##') Household
                               from city  group by state ,`State Code`""")

    # get city race data
    city_race = spark.sql("""select * from ( select   state,  race  , count from city )
    PIVOT
    (  format_number(sum(count),'##,###,###.##')  as rc_cnt  FOR (race) IN ( 'Hispanic or Latino' as Hispanic, 'Asian' ,
       'White' , 'Black or African-American' as Black,'American Indian and Alaska Native' as Native ) )
    """)
    # join the two data
    final_city = city_sum.join(city_race, on=["state"])

    # write city  table to parquet files
    final_city.write.format("parquet").mode('overwrite').save(output_data + "demography-by-state")

def process_immigration_data(output_data, file):
    """
    This function will take a parameter:
    output_data: S3 bucket where analyzed data would be stored

    This will process I94 Immigration Data.
    Creates three fact tables to further support analytics
    Files are written in parquet format
    """

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11" ) \
        .enableHiveSupport()\
        .getOrCreate()

    # read immigration data file
    imm_df = spark.read.format("com.github.saurfang.sas.spark").load(file)

    # create temp table on dataframe
    imm_df.createOrReplaceTempView("immigration")

    #read file with state name & code
    state_df = spark.read.format('csv').load('state_names.csv')

    state_df.createOrReplaceTempView("states")

    # get summary by state
    visitors_per_state = spark.sql("""  select format_number(I94YR,'####')  as year,
                                                format_number(I94MON,'##') as month,
                                                _c1 as state,
                                                visitors from ( 
                                                    select I94YR , 
                                                           I94MON , 
                                                           _c1 , 
                                                           count(*) as visitors
                                                      from immigration   , states
                                                     where trim(_c0) = trim(i94addr)
                                                     group by rollup( I94YR ,I94MON , _c1)
                                                     order by I94YR ,I94MON , _c1 
                                            )
                                   """)

    # write city  table to parquet files
    visitors_per_state.write.partitionBy("year", "month").format("parquet").mode('append').save(output_data + "visitors_per_state")

    cntry_df = spark.read.format('csv').load('origin_country.csv')
    cntry_df.createOrReplaceTempView("country")

    # get vistor by country data
    visitors_per_country = spark.sql("""  select format_number(I94YR,'####')  as year,
                                                  format_number(I94MON,'##') as month,
                                                  _c1 as county,
                                                  visitors from ( 
                                                    select I94YR , 
                                                           I94MON , 
                                                           _c1 , 
                                                           count(*) as visitors
                                                     from immigration   , country
                                                    where cast(trim(_c0)  as int) = cast(trim(I94RES) as int)
                                                 group by rollup( I94YR ,I94MON , _c1)
                                                  order by I94YR ,I94MON , _c1 
                                                              )
                                         """)

    # write city  table to parquet files
    visitors_per_country.write.partitionBy("year", "month").format("parquet").mode('append').save(output_data + "visitors_per_country")

    ports_df = spark.read.format('csv').option("delimiter", ";").load('ports.csv')

    ports_df.createOrReplaceTempView("ports")

    # get data by PORTS
    visitors_per_ports = spark.sql("""  select format_number(I94YR,'####')  as year,
                                                format_number(I94MON,'##') as month,
                                                _c1 as port,
                                                visitors from ( 
                                                    select I94YR , 
                                                           I94MON , 
                                                           _c1 , 
                                                           count(*) as visitors
                                                      from immigration INNER JOIN ports
                                                        ON ( trim(_c0)   = trim(i94port) )
                                                    group by rollup( I94YR ,I94MON , _c1)
                                                    order by I94YR ,I94MON , _c1 
                                                   )
                                              """)
    # write city  table to parquet files
    visitors_per_ports.write.partitionBy("year", "month").format("parquet").mode('append').save(output_data + "visitors_per_ports-per-port")


def main():
    """
     calls module to establish spark session.
     Source data files are located at HDFS cluster
     Processed parquet files are located at : s3a://capstone-rk/
     Calls functions process_city_data and process_immigration_data
    """
    # create spark session
    spark = create_spark_session()

    # target S3 bucket  where processed files are saved. This is hosted under my account
    output_data = "s3a://capstone-rk/"
    file1 = "i94_jun16_sub.sas7bdat"
    file2 = "i94_jul16_sub.sas7bdat"

    #process_city_data(spark, output_data)
    #process_immigration_data( output_data, file1)
    process_immigration_data( output_data, file2)
    spark.stop()


if __name__ == "__main__":
    main()

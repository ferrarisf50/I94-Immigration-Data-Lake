import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from datetime import datetime, timedelta
import numpy as np
import re
from functools import reduce
from pyspark.sql import DataFrame
import glob

def create_spark_session():
    """
    Create a Spark session.
    
    INPUT: None
    OUTPUT: Spark session
    
    """
       
    '''   
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    ''' 
    
    spark = SparkSession \
         .builder \
         .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11") \
         .enableHiveSupport() \
         .getOrCreate()
    
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")
    
    return spark



def process_i94_data(spark, filepath, outpath):
    """
    Process the i94 immigration dataset, load into Spark, transform the data, run quality check, then write to parquet file.
    
    INPUT: 
    spark - Spark session
    filepath - the directory where the file locate 
    outpath - the directory where the output parquet file locate
    
    OUTPUT:
    None
    """
    
    
    
    
   
    print("Reading i94 immigration data...")
    
    all_files = glob.glob(filepath + "/*.sas7bdat")

    li = []

    for filename in all_files:
        df = spark.read.format('com.github.saurfang.sas.spark').load(filename)
        if len(df.columns) == 34:
            i94_jun = reduce(DataFrame.drop,['validres','delete_days','delete_mexl','delete_dup','delete_visa','delete_recdup'], df)    
            li.append(i94_jun)
        else:
        #print(df.printSchema())
            li.append(df)
            
    i94 = reduce(DataFrame.unionAll, li)
    
    #i94 = spark.read.format('com.github.saurfang.sas.spark').load(filepath+filename)
    
    print("i94 immigration data has %s obs." % i94.count())
    
    get_day = udf(lambda x: datetime(1960,1,1) + timedelta(days=int(x)) if x!=None else np.nan)


    i94 = i94.withColumn("arrival_date", get_day(i94.arrdate))
    i94 = i94.withColumn("departure_date", get_day(i94.depdate))
    
    
    i94.createOrReplaceTempView("i94_view")

    i94_result = spark.sql(""" 
    
      select 

      concat(string(int(i94yr)),'-',string(int(i94mon)),'-',string(int(cicid))) as id,
      year(arrival_date) as year,
      month(arrival_date) as month,
      day(arrival_date) as day,
      int(i94cit) as citizen_code,
      int(i94res) as resident_code,
      i94port as port_code,
      arrival_date,
      departure_date,
      int(i94bir) as age,
      int(i94visa) as visa_code,
      int(biryear) as birth_year,
      gender,
      airline as airline_code,
      int(admnum) as admission_number,
      visatype as visa_type

      from i94_view
      
    """    
      #+"TABLESAMPLE (.001 PERCENT)"
    )  
     
        
        
    ####### validate the arrival date distribution    ###############
    
    print("Validate the arrival date distribution...")
    
    i94_result.createOrReplaceTempView("i94_view")

    result = spark.sql(""" 
      select arrival_date, count(*)
      from i94_view
      group by arrival_date

    """)
    
    
    print("# DATE ## COUNT #")
    for line in result.collect():
        mdy = re.findall('YEAR=(\d+).+MONTH=(\d+).+DAY_OF_MONTH=(\d+)',line[0],flags=0)
        count= line[1]
        year = mdy[0][0]
        month = str(int(mdy[0][1])+1)
        day = mdy[0][2]

        print(f"{year}-{month}-{day}: {count}")
    
    # write to parquet partitioned by year, month ,day

    i94_result.write.parquet(outpath+"i94.parquet",mode='overwrite',partitionBy=("year", "month","day"))
    
    print("I94 immigration table is completed!")
    
    
def process_dimension_data(spark, filepath, outpath):
    
    """
    Process the 6 dimension tables, load into Spark, transform the data, run quality check, then write to parquet file.
    
    INPUT: 
    spark - Spark session
    filepath - the directory where the file locate 
    filename - the source file name
    outpath - the directory where the output parquet file locate
    
    OUTPUT:
    None
    """
    
    
    ######## airlines ###################
    
    airlines = spark.read.csv(filepath+"iata_airlines.csv", header=True)
    
    airlines.createOrReplaceTempView("airlines_view")
    result = spark.sql(""" 
      select 

      IATADesignator as iata_code,
      AirlineName as airline_name,
      `Country/Territory` as airline_country

      from airlines_view 

    """)
    
    
  
    print("airlines table has %s obs." % result.count())
    
    result.write.parquet(outpath+"airlines.parquet",mode='overwrite')
    
    
    ######## ports ###################
    
    ports = spark.read.csv(filepath+"i94port.csv", header=True)
    
    ports.createOrReplaceTempView("ports_view")

    result = spark.sql(""" 
      select distinct

      START as port_code,
      city,
      state

      from ports_view

    """)  
    
    
    print("ports table has %s obs." % result.count())
    
    result.write.parquet(outpath+"ports.parquet",mode='overwrite')
    
    
    ######## modes ###################
    
    modes = spark.read.csv(filepath+"i94mode.csv", header=True)
    
    modes.createOrReplaceTempView("modes_view")

    result = spark.sql(""" 
      select distinct
      
      START as mode_code,
      LABEL as mode
      
      from modes_view

    """)  
    
    print("modes table has %s obs." % result.count())
    
    result.write.parquet(outpath+"modes.parquet",mode='overwrite')
    
    ######## countries ###################
    
    countries = spark.read.csv(filepath+"i94cit_res.csv", header=True)

    countries.createOrReplaceTempView("countries_view")

    result = spark.sql(""" 
      select distinct

      START as country_code,
      LABEL as country

      from countries_view

    """) 

    print("countries table has %s obs." % result.count())
    
    result.write.parquet(outpath+"countries.parquet",mode='overwrite')
    
    ######## states ###################
    
    states = spark.read.csv(filepath+"i94addr.csv", header=True)

    states.createOrReplaceTempView("states_view")

    result = spark.sql(""" 
      select distinct

      START as address_state,
      LABEL as state

      from states_view

    """) 

    print("states table has %s obs." % result.count())
    result.write.parquet(outpath+"states.parquet",mode='overwrite')

    ######## visas ###################
    
    visas = spark.read.csv(filepath+"i94visa.csv", header=True)

    visas.createOrReplaceTempView("visas_view")

    result = spark.sql(""" 
      select distinct

      START as visa_code,
      LABEL as visa

      from visas_view

    """) 

    print("visas table has %s obs." % result.count())
    result.write.parquet(outpath+"visas.parquet",mode='overwrite')
    
    
    
def main():
    
    config = configparser.ConfigParser()
    
    
    '''
    
  
    config.read(r'dl.cfg')
    
    dim_filepath = "./"
    i94_filepath = "../../data/18-83510-I94-Data-2016/"
    #i94_filename ="i94_apr16_sub.sas7bdat"
    
    outpath = "data/"
       '''
    
    config.read(r'/home/hadoop/dl.cfg')
    
    os.environ['AWS_ACCESS_KEY_ID'] = config.get('AWS','AWS_ACCESS_KEY_ID')
    os.environ['AWS_SECRET_ACCESS_KEY']= config.get('AWS','AWS_SECRET_ACCESS_KEY')
    bucket = config.get('S3','bucket') 
       
    
    dim_filepath = "s3a://"+bucket+"/i94/data/"
    i94_filepath = "s3a://"+bucket+"/i94/data/"
    #i94_filename ="i94_apr16_sub.sas7bdat"
    
    outpath = "s3a://"+bucket+"/i94/parquets/"
    
    
    spark = create_spark_session()
    

    process_i94_data(spark, i94_filepath, outpath)


    #process_dimension_data(spark, dim_filepath, outpath)
      
    print("All set!")


if __name__ == "__main__":
    main()
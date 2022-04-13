import os
import datetime
from pyspark.sql import SparkSession 
from pyspark.sql import functions as F
from pyspark.sql.types import DateType,IntegerType, StringType 
from pyspark.sql.window import Window


'''
i94 immigration and citytemperatures data preperation for implementing conceptual data model
In orderder to process this script three information variables which are shown below need to be filled.
This file suggested to be execute in spark standallone mode.
'''
#Process environment setup
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
os.environ['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
os.environ['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY

#Name of s3 bucket  which contains raw data
S3_BUCKET_INPUT =  ''

#Name of s3 bucket which will be used as raw data (in our case output and input paths of s3 bucket should be same)
S3_BUCKET_OUTPUT = ''

#Year of i94 nonimmigrant data
YEAR = '2016'

    
def build_spark_session():
    """
    Composing spark session 
    
    """
    spark = SparkSession.builder\
    .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
    .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    .enableHiveSupport().getOrCreate()
    return spark

S3_BUCKET_INPUT = ''
S3_BUCKET_OUTPUT = ''
input_data = S3_BUCKET_INPUT 
output_data = S3_BUCKET_OUTPUT
@F.udf (returnType = StringType())
def i94cit_converter (i94cit,input_data = S3_BUCKET_INPUT):
    """
    User defined function in order to convert i94cit and i94res int codes into feasible content
    
    Argument:
        i94cit: three letter abbreviation in the source data
        input_data: s3 bucket name
    """
    file_path = os.path.join(input_data,"I94_SAS_Labels_Descriptions.SAS")
    i94cit = int(i94cit)
    with open(file_path) as f:
        labels = f.readlines()
    country_code={}                                 
    for country in labels[10:297]:
        country = country.split("=")
        code, name = int(country[0].strip()), country[1].strip().strip("'")
        country_code[code]=name
    
    if int(i94cit) in country_code.keys() and i94cit is not None:
        return country_code[int(i94cit)]
    else:
        return None

@F.udf (returnType = StringType())
def i94addr_converter (i94addr,input_data = S3_BUCKET_INPUT):
    """
    User defined function in order to convert i94addr to proper state name.
    
    Argument:
        i94addr: two letters abbreviation element in the source data
        input_data: s3 bucket name
    """
    file_path = os.path.join(input_data,"I94_SAS_Labels_Descriptions.SAS")
    with open(file_path) as f:
        labels = f.readlines()
    state_code = {}
    for states in labels[982:1036]:
        st = states.split("=")
        code, state = st[0].strip('\t').strip("'"), st[1].strip().strip("'").strip("\t")
        state_code[code] = state
    if str(i94addr) in state_code.keys() and i94addr is not None:
        return state_code[i94addr]
    else:
        return None

@F.udf (returnType = StringType())
def i94port_converter (i94port,input_data = S3_BUCKET_INPUT):
    """
    User defined function in order to convert i94port to port name and state of the port.
    
    Argument:
        i94port: three letters abbreviation element in the source data
        input_data: s3 bucket name
    """
    file_path = os.path.join(input_data,"I94_SAS_Labels_Descriptions.SAS")
    with open(file_path) as f:
        labels = f.readlines()
    port_code = {}
    for cities in labels[303:962]:
        cit = cities.split("=")
        code, city_state = cit[0].strip("\t").strip().strip("'"), cit[1].split(",")
        port_code[code] = city_state[0].strip("\t").strip("'")
  
    return port_code[i94port] 

    
@F.udf (returnType = DateType())
def sasdate_converter (sas_date):
    """
    Converting SAS date format into date time format for arrival and departure dates.
    
    Argument:
        sas_date: SAS date format
    """
    if sas_date is not None:
        return datetime.date(1960,1,1)+datetime.timedelta(sas_date)

@F.udf (returnType = StringType())    
def i94mode_converter (mode):
    """
    Converting i94mode data into feasible format
    
    Argument:
        mode: Mode which is descripted in I94_SAS_labels_Descriptions
    
    """
    mode_dict = {1:'Air',2:'Sea', 3:'Land'}
    if mode in mode_dict:
        mode = mode_dict[mode]
    else:
        mode = None
    return mode

@F.udf (returnType = StringType())
def i94visa_converter (visa):
    """
    Converting i94visa data into feasible format
    
    Arument:
        visa: Visa code stated in i94visa column in the data
    """
    visa_dict = {1:'Business',2:'Pleasure',3:'Student'}
    if visa in visa_dict:
        visa = visa_dict[visa]
    else:
        visa = None
    return visa

def convert_data_im94(spark,input_data,output_data,year):
    """
    Cleaning I94 immigration data in order to create more reliable data. 
    Headers of the data file renamed into more sense and explanatory titles.
    Necessery data due to conceptual model filtered and processed.
    In process, related sas7bat file converted to the parquet file to perform
    faster transactions.
    
    
    Arguments:
        spark : SparkSession object
        input_data : S3 bucket source 
        output_data : S3 bucket aim
        year : execution year of the etl
    
    """    
    # Gathering data from the source    
    main_data = f"../../data/18-83510-I94-Data-{year}"
    s3_pathway = os.path.join(input_data,main_data)
    file=os.listdir(s3_pathway)

    for f in file:
        input_loc = os.path.join(s3_pathway,f)
        df= spark.read.parquet(input_loc)
        df = spark.read.format('com.github.saurfang.sas.spark').load(input_loc)
        
        df = df.withColumn('record_year',df['i94yr'].cast(IntegerType()))\
            .withColumn('record_month',df['i94mon'].cast(IntegerType()))\
            .withColumn('arrival_date',sasdate_converter(df['arrdate']))\
            .withColumn('departure_date',sasdate_converter(df['depdate']))\
            .withColumn('age',df['i94bir'].cast(IntegerType()))\
            .withColumn('country_born',i94cit_converter(df['i94cit']))\
            .withColumn('country_residence',i94cit_converter(df['i94res']))\
            .withColumn('visa_purpose',i94visa_converter(df['i94visa']))\
            .withColumn('birth_year', df['biryear'].cast(IntegerType()))\
            .withColumn('port_code',df['i94port'])\
            .withColumn('port_city',i94port_converter(df['i94port']))\
            .withColumn('record_id',df['cicid'].cast(IntegerType()))\
            .withColumn('state_abbr',df['i94addr'])\
            .withColumn('state',i94addr_converter(df['i94addr']))\
            .withColumn('mode',i94mode_converter(df['i94mode']))
        
        #Creating new columns for partition in data file
        df =df.withColumn("arrival_year", F.year("arrival_date"))\
            .withColumn("arrival_month", F.month("arrival_date"))\
            .withColumn("arrival_day", F.dayofmonth("arrival_date"))

        #Selecting data occording to conceptual data model
        df_imm = df.select(['record_year','record_month','arrival_date'\
                   ,'arrival_year','arrival_month','arrival_day'\
                   ,'departure_date','age','gender','country_born'\
                   ,'country_residence','state_abbr','state','visa_purpose','visatype'\
                   ,'birth_year','port_code','port_city','occup','mode','airline','fltno','record_id'])
                           
        #Saving partitioned data to preferred destination as Apache parquet file
        output_path = os.path.join(output_data,"clean_i94_data")
        df_imm.write.mode("append").partitionBy("arrival_year","arrival_month","arrival_day").parquet(output_path)
        
def convert_data_citytemp (spark,input_data,output_data):
    """
    City temperatures in the US from 'GlobalLAndTemparaturesByCity.csv',
    cleaned in order to get valid data and
    converted into parquet file format.
    
    Arguments:
        spark : SparkSession object
        input_data : S3 bucket source 
        output_data : S3 bucket aim
    
    """
    fname = os.path.join(input_data,"data2/GlobalLandTemperaturesByCity.csv")
    df_temp = spark.read.option("header",True).option("inferSchema",True).csv(fname)
    df_temp = df_temp.where(df_temp.Country == 'United States')
    df_temp = df_temp.where(df_temp.dt > datetime.datetime(1999,12,1)).withColumnRenamed("dt","date")
    df_temp = df_temp.where(df_temp.AverageTemperature.isNotNull())
    df_temp= df_temp.withColumn("month",F.month("date"))
    df_temp = df_temp.groupBy("City","Country","Latitude","Longitude","month")\
             .agg(F.avg("AverageTemperature").alias("avg_temperature")\
                  ,F.avg("AverageTemperatureUncertainty").alias("avg_tempuncertainity"))
                           
    #Saving partitioned data to preferred destination as Apache parquet file
    output_path = os.path.join(output_data,"clean_temp_data")
    df_temp.write.mode("overwrite").parquet(output_path)  
    

def main ():
    """
    In order to execute functions accordingly this function has been used
    """
    
    spark = build_spark_session()
    input_data = S3_BUCKET_INPUT
    output_data = S3_BUCKET_OUTPUT
    year= YEAR
    convert_data_im94(spark,input_data,output_data,year)
    convert_data_citytemp(spark,input_data,output_data)
    spark.stop()
    
if __name__=="__main__":
    main()
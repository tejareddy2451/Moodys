from audioop import getsample
import string
import boto3
from boto3.dynamodb.conditions import Key,Attr
from ingestion_framework.constants import projectConstants
from ingestion_framework.connector_framework.connector_utils import *
from pyspark.sql.functions import *
from ingestion_framework.scripts import job_initializer
import random
import numpy as np
from pyspark.sql.types import IntegerType
from ingestion_framework.utils import logger_util
import datetime
from datetime import date
from ingestion_framework.utils import *



@logger_util.common_logger_exception
def celltocell_validation(environ,inventoryID,XmlName,spark,logger,AppName):

    print(f'celltocellvalidation_util - celltocell_validation - Started - PipelineID{inventoryID}')
    print(AppName)
    print(inventoryID)
    inv_dict = InventoryReader.get_pipeline_dict_from_yaml(AppName, inventoryID,'1')
    print(inv_dict)

    job_start_datetime = datetime.now().replace(microsecond=0) 
    job_end_datetime = datetime.now().replace(microsecond=0)
    new_query = common_util.get_source_query(spark, logger, inv_dict, inventoryID, job_start_datetime, job_end_datetime,environ)
    inv_dict['source_query'] = new_query
    print(f"new_query{new_query}")

    src_dict=InventoryReader(inv_dict).get_source_connector_config()
    tgt_dict=InventoryReader(inv_dict).get_target_connector_config()


    src_connector=ConnectorSupplier(spark, logger, environ).get_connector(src_dict)
    tgt_connector=ConnectorSupplier(spark, logger, environ).get_connector(tgt_dict)


    

    src_dataframe=src_connector.read_data()
    tgt_dataframe=tgt_connector.read_data()

    #src_dataframe = spark.read.option("header",True).csv("src.csv")
    #tgt_dataframe = spark.read.option("header",True).csv("tgt.csv")
    
    print("src data frame")
    #src_dataframe.show()
    print(f"src data frame{src_dataframe}")
    print("tgt data frame")
    print(f"tgt data frame{tgt_dataframe}")
    #tgt_dataframe.show()
    
    primary_key=''
    #primary_key=get_primary_key(environ,inventoryID)
    if not primary_key:
        newprimary=formPrimaryKey(src_dataframe,tgt_dataframe,spark)
        '''new_src_data=src_dataframe.select(concat(src_dataframe.id,src_dataframe.name,src_dataframe.age)
        .alias("FullID"),"id","name","age")'''
        new_src_data=newprimary[1]
        new_src_data.show()
    #primary_key="id"

    src_minus_tgt=get_minus_dataframe(src_dataframe,tgt_dataframe)
    tgt_minus_src=get_minus_dataframe(tgt_dataframe,src_dataframe)
    
    print("src_minus_tgt")
    print(src_minus_tgt.count())
    src_minus_tgt.show()
    print("tgt_minus_src")
    tgt_minus_src.show()

    try:
        perform_validation(src_minus_tgt,tgt_minus_src,environ,inventoryID,spark,primary_key)
    except Exception as err:
        print(f'celltocellvalidation_util - celltocell_validation - Ended - PipelineID{inventoryID}')
        print(f'celltocellvalidation_util - celltocell_validation - Ended - {err}')

    print(f'celltocellvalidation_util - celltocell_validation - Ended - PipelineID{inventoryID}')

@logger_util.common_logger_exception
def formPrimaryKey(src_dataframe,tgt_dataframe,spark):
    print(f'celltocellvalidation_util - formPrimaryKey - Started')
    print("formprimarykey")
    dataarray=[]
    src_columns=src_dataframe.columns
    tgt_columns=tgt_dataframe.columns
    src_column_string=''
    tgt_column_string=''
    for srccol in src_columns:
        if src_column_string=='':
            src_column_string=srccol
            print(src_column_string)
        else:
            src_column_string+=","+srccol
            print(src_column_string)

    src_concat=src_column_string

    src_column_string='';
    for srccol in src_columns:
            src_column_string+=",'"+srccol+"'"

    for tgtcol in tgt_columns:
        if tgt_column_string=='':
            tgt_column_string=tgtcol
        else:
            tgt_column_string+=","+tgtcol

    tgt_concat=tgt_column_string

    tgt_column_string='';

    for tgtcol in tgt_columns:
            tgt_column_string+=",'"+tgtcol+"'"

    print("src_column_string")
    print(src_column_string)
    print("src concat")
    print(src_concat)
    #new_src_dataframe=src_dataframe.select(concat(src_concat).alias('FullKey')+','+src_concat)
    #new_src_dataframe=src_dataframe.select(src_concat)
    #new_tgt_dataframe=tgt_dataframe.select(concat(tgt_concat).alias('FullKey')+','+tgt_concat)
    #new_tgt_dataframe=tgt_dataframe.select(tgt_concat)
    src_dataframe.createTempView("src")
    tgt_dataframe.createTempView("tgt")
    srcquery="select concat("+src_concat +") as 'FullKey',"+ src_concat +" from src"
    print(srcquery)
    new_src_dataframe = spark.sql("select concat("+src_concat +") FullKey,"+ src_concat +" from src")
    print(new_src_dataframe)
    new_tgt_dataframe = spark.sql("select concat("+tgt_concat +") FullKey,"+ tgt_concat +" from tgt")
    print(new_tgt_dataframe)
    

    dataarray.append("FullKey")
    dataarray.append(new_src_dataframe)
    dataarray.append(new_tgt_dataframe)
    print(new_src_dataframe)
    print(new_tgt_dataframe)
    print(f'celltocellvalidation_util - formPrimaryKey - Ended')
    return dataarray

@logger_util.common_logger_exception
def perform_validation(src_minus_tgt,tgt_minus_src,environ,inventoryID,spark,primary_key):

    print(f'celltocellvalidation_util - perform_validation - Started')

    if src_minus_tgt.count()>0 or tgt_minus_src.count()>0:

        if primary_key:
            src_key=src_minus_tgt.select(primary_key)
            tgt_key=tgt_minus_src.select(primary_key)
            common_key=getcommon_data(src_key,tgt_key)
            common_key.show()
            sort_common_key=common_key.sort('id')
            print("sort_common_key")
            sort_common_key.show()
            tgt_key_missing=get_minus_dataframe(src_key,tgt_key)
            tgt_key_missing.show()
            src_key_missing=get_minus_dataframe(tgt_key,src_key)
            src_key_missing.show()
            if(sort_common_key.count()<=1000):
                print("Inside sort_common_key")
                #sort_common_key.withColumnRenamed("id","idcol")
                sort_common_key_copy = sort_common_key.alias('sort_common_key_copy')
                try:
                    comparison(src_minus_tgt,tgt_minus_src,sort_common_key,sort_common_key_copy,primary_key)
                except Exception as err:
                    print(f"Error in comparison {err}")

                
            else:
                randomdata=randomsample(sort_common_key,sort_common_key.count(),1000)
                print(len(randomdata))
                #sampledf = spark.createDataFrame(randomdata, ["id"])
                #sampledf.show()
                rdd1 = spark.sparkContext.parallelize(randomdata)
                rdd2 = rdd1.map(lambda x: [int(i) for i in x])
                keys = rdd2.toDF(["idnew"])
                sort_common_key_copy = keys.alias('sort_common_key_copy')
                try:
                    comparison(src_minus_tgt,tgt_minus_src,keys,sort_common_key_copy,primary_key)
                except Exception as err:
                    print(f"Error in comparison {err}")


    else:
        print("no mismatch")
    print(f'celltocellvalidation_util - perform_validation - Started')

@logger_util.common_logger_exception
def comparison(src_minus_tgt,tgt_minus_src,sort_common_key,sort_common_key_copy,primary_key):
    #data_array =  np.array(sort_common_key.select("idNew").collect())
    print(f'celltocellvalidation_util - comparison - Started')
    print("inside comparison")
    src_data=src_minus_tgt.filter(src_minus_tgt.id.isin(sort_common_key.select('idnew')))
                #tgt_data=src_minus_tgt.filter(src_minus_tgt.id.isin(sort_common_key.id))
    print("src_data")
    src_data.show()
    tgt_minus_src.show()
    sort_common_key_copy.show()
    #sort_common_key_copy.withColumnRenamed("id","idcol")
    tgt_data=tgt_minus_src.filter((tgt_minus_src.id).isin(sort_common_key_copy.id))
    #tgt_data=src_minus_tgt.filter(src_minus_tgt.id.isin(sort_common_key.idNew))
    print("tgt_data")
    tgt_data.show()
    src_data_sort=src_data.sort(primary_key)
    tgt_data_sort=tgt_data.sort(primary_key)
    src_data_sort.show()
    tgt_data_sort.show()
    src_columns=src_data.columns
    src_rearranged=''
    for srccol in src_columns:
        #print(srccol)
        
        if not src_rearranged:
            src_rearranged="\""+srccol+"\""
        else:
            if str(srccol).lower()==primary_key.lower():
                    src_rearranged="\""+srccol+"\","+src_rearranged
            src_rearranged+=",\""+srccol+"\""
        
        print(src_rearranged)
    clmArray=[]
    clmArray.append("id")
    clmArray.append("name")
    clmArray.append("age")
    src_data_sort_rearranged = src_data_sort.select(clmArray)
    src_data_sort_rearranged.show()
    tgt_columns=tgt_data.columns
    tgt_rearranged=''
    for tgtcol in tgt_columns:
        # print(tgtcol)
        
        if not src_rearranged:
            tgt_rearranged="\""+tgtcol+"\""
        else:
            if str(tgtcol).lower()==primary_key.lower():
                    tgt_rearranged="\""+tgtcol+"\","+tgt_rearranged
            tgt_rearranged+=",\""+tgtcol+"\""
        
        print(tgt_rearranged)
    tgt_data_sort_rearranged = tgt_data_sort.select(clmArray)
    tgt_data_sort_rearranged.show()
    mismatch=identifyMismatch(src_data_sort_rearranged,tgt_data_sort_rearranged,primary_key)
    print(f'celltocellvalidation_util - comparison - Ended')
    return mismatch
@logger_util.common_logger_exception
def identifyMismatch(df1,df2,key):
    print(f'celltocellvalidation_util - identifyMismatch - Started')

    conditions_ = [when(df1[c]!=df2[c], lit(c)).otherwise("") for c in df1.columns if c != key]

    select_expr =[
                col(key), 
                *[df2[c] for c in df2.columns if c != key], 
                array_remove(array(*conditions_), "").alias("column_names")]

    print(f'celltocellvalidation_util - identifyMismatch - Ended')
    return df1.join(df2, key).select(*select_expr)

@logger_util.common_logger_exception
def getcommon_data(df1,df2):
    return df1.intersect(df2)

@logger_util.common_logger_exception
def get_minus_dataframe(dataframe1,dataframe2):
    return dataframe1.subtract(dataframe2)

@logger_util.common_logger_exception
def get_primary_key(env,pipelineid):
    print(f'celltocellvalidation_util - get_primary_key - Started')
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    tableCol = dynamodb.Table("datamesh-inventory-pipeline--"+env)
    response1 = tableCol.scan(
        FilterExpression=Attr('pipeline_id').eq(pipelineid)
    )
    ruledict={}
    fullColumnName=""
    print(response1["Items"])
    for res in response1["Items"]:
        primary_key=res['primary_key_set']
      
    print(f'celltocellvalidation_util - get_primary_key - Ended')
    return primary_key

@logger_util.common_logger_exception
def metadata_validation(environ,inventoryID):
    print(f'celltocellvalidation_util - metadata_validation - Started')
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    tableCol = dynamodb.Table("datamesh-inventory-pipeline-col-"+environ)
    response1 = tableCol.scan(
        FilterExpression=Attr('pipeline_id').eq(inventoryID)
    )
    ruledict={}
    fullColumnName=""
    print(response1["Items"])
    for res in response1["Items"]:
        src_columnname=res['source_column_name']
        tgt_columnname=res['target_column_name']

        src_datatype=res['source_datatype']
        tgt_datatype=res['target_datatpe']

        src_std_type=get_standard_datatpe(src_datatype)
        tgt_std_type=get_standard_datatpe(tgt_datatype)

        if src_std_type==tgt_std_type:
            print(src_columnname+"******"+tgt_columnname)
            print(src_datatype+"******"+tgt_datatype)
            print(src_std_type+"******"+tgt_std_type)
            print("matching")
        else:
            print(src_columnname+"******"+tgt_columnname)
            print(src_datatype+"******"+tgt_datatype)
            print(src_std_type+"******"+tgt_std_type)
            print("not matching")

    print(f'celltocellvalidation_util - metadata_validation - Ended')
@logger_util.common_logger_exception
def randomsample(df,top,count):
    print("randomsample")
    df.show()
    data_array =  np.array(df.collect())
    sample_data=[]
    data=[]
    while(True):
        if len(data)<count:
            randomnumber=random.randint(1,top-10)
            if randomnumber not in data:
                data.append(randomnumber)
                sample_data.append(data_array[randomnumber])
        else:
            break
    
    return sample_data

@logger_util.common_logger_exception
def get_standard_datatpe(datatype):

    number=["int","decimal"]
    string=["char","varchar","varchar2","string"]
    date=["timestamp","datetime","date"]
    
    print("get_standard_datatpe")
    print(datatype.lower())
    
    if datatype.lower() in number:
        return "number"
    elif datatype.lower() in string:
        return "string"
    else:
        return "date"

def main():
    spark = job_initializer.initializeSpark("trial")
    logger = job_initializer.getGlueLogger(spark)
    celltocell_validation("","","",spark,logger,"")

if __name__ == "__main__":
    main()
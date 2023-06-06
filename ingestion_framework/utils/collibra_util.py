'''
Module Name : collibra_util.py
Called From Wrapper Module : datamesh_collibra_raw.py
'''
import boto3, json, ast, time, requests
import pyspark.sql.functions as F
from pyspark.sql.functions import when, explode, split, translate, lower
from pyspark.sql.types import StructType, StructField, StringType
from pandas import json_normalize

from ingestion_framework.scripts import job_initializer
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants

# Module Constants
SCRIPT_NAME = "collibra_util"



# The below Payload Json should match the csv that gets generated down the line
COLLIBRA_RAW_TEMPLATE = {
      "sendNotification": "False",
      "headerRow": "True",
      "separator": ",",
      "template": """[
  {
    "resourceType": "Asset",
    "identifier": {
      "name": "${1}",
      "domain": {
        "name": "${5}",
        "community": {
          "name": "${3}"
        }
      }
    },
    "displayName": "${2}",
    "type": {
      "name": "${6}"
    },
      "attributes" : {

        "Data Type" :[{
          "value" : "${7}" }]
          ,
        "Data Type Precision" :[{
          "value" : "${8}" }]
          ,
        "Size" :[{
          "value" : "${9}" }]
          ,
        "Data Classification" :[{
          "value" : "${10}" }]
          ,
        "File Location" :[{
          "value" : "${11}" }]
          ,
        "Format" :[{
          "value" : "${12}" }]
          ,
        "Description" :[{
          "value" : "${13}" }]
      },
    "relations": {
      "00000000-0000-0000-0000-000000007042:TARGET": [
        {
          "name": "${14}",
          "domain": {
            "name": "${17}",
            "community": {
              "name": "${16}"
            }
          }
        }
      ]
    }
  }
]"""
  }
def upload_csv_to_collibra(url, token, s3_bucket, s3_prefix):
    ''' This funciton will send a post request to collibra | input: csv + payload_json | output : API response json '''
    # Sends the post request
    file = boto3.resource(projectConstants.S3).Object(s3_bucket,s3_prefix).get()
    data = file['Body'].read()
    csv_feed =[('file', data)]
    # set header with token
    headers = {'Authorization': 'Basic {}'.format(token)}
    response = requests.request("POST", url, headers=headers, data=COLLIBRA_RAW_TEMPLATE, files= csv_feed)
    response_json  = json_normalize(response.json())
    final_resp = response_json['id'].loc[0]
    return final_resp

def get_collibra_job_details(jobs_url, token,  collibra_job_id):
    ''' Gets the response from Collibra along with error message  '''
    jobs_url_id  = jobs_url.format(collibra_job_id)
    headers   = {'Authorization': 'Basic {}'.format(token)}
    resp_json = requests.request("GET", jobs_url_id, headers=headers)
    return resp_json.json()

def extract_bucket_path(tgt_envs, job_env):
    ''' Extract target file path from target_environment tag '''
    tgt_env_details = {}
    tgt_envs        = tgt_envs.replace('null','None')
    tgt_envs_list   = ast.literal_eval(tgt_envs)

    for tgt_env_dict in tgt_envs_list:
        environment_name = tgt_env_dict['environment_name'].upper()
        if job_env.upper() == environment_name:
            for key,value in tgt_env_dict.items():
                tgt_env_details[key] = value
                target_file_path   = "{}{}{}".format('s3://',tgt_env_dict['target_bucket_name'],'/data/final')
    return target_file_path



def generate_collibra_raw_csv(job_env, aws_account_id, aws_region):
    ''' Generates csv file for collibra | Input : dynamoDB | output : csv file (s3) '''
    function_name = 'generate_collibra_raw_csv'
    # variables
    collibra_csv_output_path      = f"s3a://datamesh-ingfw-{aws_account_id}-{job_env}-{aws_region}/config/collibra/datamesh_collibra_raw.csv"
    # lambdas
    udf_func = F.udf(lambda test_str: extract_bucket_path(test_str, job_env))
    
    # main control logic flows here
    spark = job_initializer.initializeSpark(projectConstants.COLLIBRA)
    print(f'{SCRIPT_NAME} - {function_name} - Process started')
    # Connect to dynamoDB
    print(f'{SCRIPT_NAME} - {function_name} - Connection to dynamo db  started')
    dynamodb = boto3.resource(dynamodbConstants.DYNAMO_DB, region_name=projectConstants.US_EAST_1)
    print(f'{SCRIPT_NAME} - {function_name} - Connection to dynamo db established :{dynamodb}')
    # create encoder to account for non json serializable objects
    EncoderMethod = lambda self, obj: str(obj)
    EncoderMap = {projectConstants.DEFAULT: EncoderMethod}
    Encoder = type(projectConstants.ENCODER, (json.JSONEncoder,), EncoderMap)
    # read the inventory table and column table for pipeline data
    try:
      #1 inventory table extract
      inventory_table       = dynamodb.Table(f'datamesh-inventory-pipeline-{job_env}')
      inventory_table_scan  = inventory_table.scan()
      inventory_table_items = inventory_table_scan[dynamodbConstants.TABLE_ITEMS]
      while dynamodbConstants.LAST_EVALUATED_KEY in inventory_table_scan:
          inventory_table_scan = inventory_table.scan(ExclusiveStartKey=inventory_table_scan[dynamodbConstants.LAST_EVALUATED_KEY])
          inventory_table_items.extend(inventory_table_scan[dynamodbConstants.TABLE_ITEMS])

      #2 pipeline-col table extract
      pipeline_col_table        = dynamodb.Table(f'datamesh-inventory-pipeline-col-{job_env}')
      pipeline_col_table_scan   = pipeline_col_table.scan()
      pipeline_column_table_items  = pipeline_col_table_scan[dynamodbConstants.TABLE_ITEMS]
      while dynamodbConstants.LAST_EVALUATED_KEY in pipeline_col_table_scan:
          pipeline_col_table_scan = pipeline_col_table.scan(ExclusiveStartKey=pipeline_col_table_scan[dynamodbConstants.LAST_EVALUATED_KEY])
          pipeline_column_table_items.extend(pipeline_col_table_scan[dynamodbConstants.TABLE_ITEMS])

      #3 inventory classification table extract
      classification_table       = dynamodb.Table(f'datamesh-inventory-classification-{job_env}')
      classification_table_scan  = classification_table.scan()
      classification_table_items = classification_table_scan[dynamodbConstants.TABLE_ITEMS]
      while dynamodbConstants.LAST_EVALUATED_KEY in classification_table_scan:
          class_table_scan = classification_table.scan(ExclusiveStartKey=classification_table_scan[dynamodbConstants.LAST_EVALUATED_KEY])
          classification_table_items.extend(class_table_scan[dynamodbConstants.TABLE_ITEMS])

      # extract(list) -> pyspark dataframe
      inventory_records_json  = json.dumps(inventory_table_items, cls=Encoder)
      inventory_records_df    = spark.read.json(spark.sparkContext.parallelize([inventory_records_json]))

      col_records_json        = json.dumps(pipeline_column_table_items, cls=Encoder)
      col_records_df          = spark.read.json(spark.sparkContext.parallelize([col_records_json]))

      class_records_json      = json.dumps(classification_table_items, cls=Encoder)
      class_records_df        = spark.read.json(spark.sparkContext.parallelize([class_records_json]))

      inventory_records_df=inventory_records_df.select('domain_name' , 'pipeline_id' , 'pipeline_desc' , 'active' , 'version_number' , 'target_file_name' , 'target_table_name' , 'target_environments') \
                                               .filter(inventory_records_df.active == "true") \
                                               .filter(inventory_records_df.status == "published")

      inventory_records_df = inventory_records_df.withColumn('table_name'  , F.concat_ws('' , F.col('target_file_name') , F.col('target_table_name')))
      inventory_records_df = inventory_records_df.withColumn('domain_name',
      F.when(F.col('domain_name')  == 'esg'       , 'DataLake_ESG') \
      .when(F.col('domain_name') == 'oem'       , 'DataLake_OEM') \
      .when(F.col('domain_name') == 'external'  , 'DataLake_External') \
      .when(F.col('domain_name') == 'finance'   , 'DataLake_Finance') \
      .when(F.col('domain_name') == 'cross'     , 'DataLake_Cross') \
      .when(F.col('domain_name') == 'reference' , 'DataLake_Reference') \
      .when(F.col('domain_name') == 'ratings' , 'DataLake_Ratings') \
      .otherwise(F.col('domain_name')))

      col_records_df=col_records_df.select('column_id','target_column_name','target_datatype','target_length','target_precision','target_datetime_format', 'pipeline_id','version_number')
      class_records_df=class_records_df.select('classification_level','column_id','business_description','version_number')

      class_col_df = col_records_df.join(class_records_df,col_records_df.column_id   ==  class_records_df.column_id , "left")
      class_col_df = class_col_df.join(inventory_records_df,class_col_df.pipeline_id ==  inventory_records_df.pipeline_id)
      class_col_df = class_col_df.withColumn('datatype'  ,  F.lower(       F.col('target_datatype'))     ) \
                                 .withColumn('full_name' ,  F.concat_ws('',F.col('target_file_name'      ) \
                                                                          ,F.col('target_table_name'))   ) \
                                 .withColumn("location"  ,  udf_func(      F.col('target_environments')) )
      
      drop_tup = ('pipeline_id','column_id','active','version_number','target_file_name','target_table_name','target_datatype')
      class_col_df = class_col_df.drop(*drop_tup)
      # removing duplicates based on the primary fields
      class_col_df = class_col_df.dropDuplicates(["domain_name","table_name","target_column_name","full_name"])
      class_col_df = class_col_df.na.fill(value="INTERNAL" , subset = ["classification_level"])
      class_col_df = class_col_df.na.fill(value="0"        , subset = ["target_length"])
      # Sorting is needed since Table assets should be loaded first  
      final_df = class_col_df.select( \
        ['domain_name', 'table_name','target_column_name','datatype','target_length','target_precision','target_datetime_format','classification_level','business_description','pipeline_desc','full_name',"location"] \
          ).sort(['domain_name', 'table_name'], ascending=True)

      collibra_table_df = final_df.groupBy('domain_name', 'table_name','pipeline_desc',"full_name",'location') \
        .agg(F.collect_list(F.to_json(F.struct( \
              'target_column_name','datatype','target_length','target_precision','target_datetime_format','classification_level','business_description','pipeline_desc','full_name','location'))) \
                .alias('aggregate'))

      collibra_table_df = collibra_table_df.drop(F.col('aggregate'))
      # Populate rows for Asset Type == Table
      collibra_table_df = collibra_table_df.withColumn('Full Name',                      F.col('full_name')) \
                                           .withColumn('Name',                           F.col('table_name')) \
                                           .withColumn('Community',                      F.col('domain_name')) \
                                           .withColumn('Domain Type',                    F.lit('Physical Data Dictionary')) \
                                           .withColumn('Domain',                         F.lit('Raw')) \
                                           .withColumn('Asset Type',                     F.lit('Table')) \
                                           .withColumn('Data Type',                      F.lit('')) \
                                           .withColumn('Data Type Precision',            F.lit('')) \
                                           .withColumn('Size',                           F.lit('')) \
                                           .withColumn('Data Classification',            F.lit('')) \
                                           .withColumn('File Location',                  F.concat_ws('/',F.col('location'),F.col('table_name'))) \
                                           .withColumn('Format',                         F.lit('')) \
                                           .withColumn('Description',                    F.col('pipeline_desc')) \
                                           .withColumn('is part of [Table] > Name',      F.lit('')) \
                                           .withColumn('is part of [Table] > Full Name', F.lit('')) \
                                           .withColumn('is part of [Table] > Community', F.lit('')) \
                                           .withColumn('is part of [Table] > Domain',    F.lit(''))
      # Populate rows for Asset Type == Column
      collibra_cols_df = final_df.withColumn('Full Name',                      F.concat_ws(' > ',F.col('target_column_name'),F.col('table_name'))) \
                                 .withColumn('Name',                           F.col('target_column_name')) \
                                 .withColumn('Community',                      F.col('domain_name')) \
                                 .withColumn('Domain Type',                    F.lit('Physical Data Dictionary')) \
                                 .withColumn('Domain',                         F.lit('Raw')) \
                                 .withColumn('Asset Type',                     F.lit('Column')) \
                                 .withColumn('Data Type',                      F.col('datatype')) \
                                 .withColumn('Data Type Precision',            F.col('target_precision')) \
                                 .withColumn('Size',                           F.col('target_length')) \
                                 .withColumn('Data Classification',            F.col('classification_level')) \
                                 .withColumn('File Location',                  F.lit('')) \
                                 .withColumn('Format',                         F.col('target_datetime_format')) \
                                 .withColumn('Description',                    F.col('business_description')) \
                                 .withColumn('is part of [Table] > Name',      F.col('table_name')) \
                                 .withColumn('is part of [Table] > Full Name', F.concat_ws(' > ',F.col('target_column_name'),F.col('table_name'))) \
                                 .withColumn('is part of [Table] > Community', F.col('domain_name')) \
                                 .withColumn('is part of [Table] > Domain',    F.lit('Raw'))
      # Drop all unwanted columns which is not used in csv
      drop_cols_x = ('domain_name', 'table_name','target_column_name','datatype','target_precision','target_length','target_datetime_format','classification_level','pipeline_desc','target_environments')
      collibra_table_df = collibra_table_df.drop(*drop_cols_x)
      drop_cols_y = ('domain_name', 'table_name','target_column_name', 'datatype','target_precision','target_length','target_datetime_format', 'classification_level','business_description', 'pipeline_desc','target_environments')
      collibra_cols_df = collibra_cols_df.drop(*drop_cols_y)
      # Join Column & Table asset types for the Data Type Field
      collibra_csv_df = collibra_table_df.union(collibra_cols_df)

      # convert Datamesh data types to collibra accepted formats
      collibra_csv_df =  collibra_csv_df.withColumn('Data Type' , F.regexp_replace('Data Type' , 'timestamp'         , 'Date Time')     ) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'string'            , 'Text')          ) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'int'               , 'Whole Number')  ) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'boolean'           , 'True/False')    ) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'datetime'          , 'Date Time')     ) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'bigDecimal Number' , 'Decimal Number')) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'bigWhole Number'   , 'Whole Number')  ) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'date'              , 'Date')          ) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'decimal'           , 'Decimal Number')) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'varchar'           , 'Text')) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'double'            , 'Decimal Number')) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'char'              , 'Text')) \
                                        .withColumn('Data Type' , F.regexp_replace('Data Type' , 'smallWhole Number' , 'Whole Number'))
                                        
      # drop unwanted columns
      collibra_csv_df = collibra_csv_df.drop('full_name','location')
      # write to /config/collibra location
      collibra_csv_df.toPandas().to_csv(collibra_csv_output_path, header=True,index=False)
      return True

    except Exception as err:

        print(f'{SCRIPT_NAME} - {function_name} - Failed with error: {err}')
        raise Exception(f'{SCRIPT_NAME}  - Failed with error: {err}') from err


def load_collibra_raw(job_env, aws_region, collibra_token, job_monitoring_url, collibra_url):
    '''
    Prepare Collibra csv -> Push to collibra -> get response from Collibra
    '''
    function_name   = 'load_collibra_raw'
    aws_account_id  = boto3.client(projectConstants.STS).get_caller_identity()[projectConstants.ACCOUNT]
    s3_csv_location = f"datamesh-ingfw-{aws_account_id}-{job_env}-{aws_region}"
    s3_csv_prefix   = "config/collibra/datamesh_collibra_raw.csv"
    generate_collibra_raw_csv_response = False
    generate_collibra_raw_csv_response = generate_collibra_raw_csv(job_env, aws_account_id, aws_region)

    if generate_collibra_raw_csv_response:
        collibra_job_id=upload_csv_to_collibra(collibra_url, collibra_token,s3_csv_location,s3_csv_prefix)
        print(f"{SCRIPT_NAME} - {function_name} - Collibra Response Job ID: {collibra_job_id}")
        print(f"{SCRIPT_NAME} - {function_name} - Waiting for 20 sec(s)")
        time.sleep(20)
        job_resp_json = get_collibra_job_details(job_monitoring_url, collibra_token, collibra_job_id)
        if job_resp_json['result'] == 'FAILURE':
          print(f"{SCRIPT_NAME} - {function_name} - Collibra Response : Failure| Message :{job_resp_json['message']}")
          raise SystemExit(f"{SCRIPT_NAME} - {function_name} - Collibra Response : Failure| Message :{job_resp_json['message']}")
        else:
          print(f"{SCRIPT_NAME} - {function_name} - {job_resp_json['message']}")

def generate_collibra_processed_csv(job_env, s3_csv_location, s3_csv_prefix):
    function_name = 'generate_collibra_processed_csv'
    collibra_csv_output_path = f"s3a://{s3_csv_location}/{s3_csv_prefix}"
    spark = job_initializer.initializeSpark(projectConstants.COLLIBRA)
    print(f'{SCRIPT_NAME} - {function_name} - Process started')
    # Connect to dynamoDB
    print(f'{SCRIPT_NAME} - {function_name} - Connection to dynamo db  started')
    dynamodb = boto3.resource(dynamodbConstants.DYNAMO_DB, region_name=projectConstants.US_EAST_1)
    print(f'{SCRIPT_NAME} - {function_name} - Connection to dynamo db established :{dynamodb}\n')
    # create encoder to account for non json serializable objects
    EncoderMethod = lambda self, obj: str(obj)
    EncoderMap = {projectConstants.DEFAULT: EncoderMethod}
    Encoder = type(projectConstants.ENCODER, (json.JSONEncoder,), EncoderMap)
    # read the inventory table and column table for pipeline data
    try:
        # 1 lineage-tracker table extract
        lineage_table = dynamodb.Table(dynamodbConstants.LINEAGE_TRACKER_TABLE.format(job_env))
        lineage_table_scan = lineage_table.scan()
        lineage_table_items = lineage_table_scan[dynamodbConstants.TABLE_ITEMS]
        while dynamodbConstants.LAST_EVALUATED_KEY in lineage_table_scan:
            lineage_table_scan = lineage_table.scan(
                ExclusiveStartKey=lineage_table_scan[dynamodbConstants.LAST_EVALUATED_KEY])
            lineage_table_items.extend(lineage_table_scan[dynamodbConstants.TABLE_ITEMS])

        # extract(list) -> pyspark dataframe
        lineage_records_json = json.dumps(lineage_table_items, cls=Encoder)
        lineage_records_df = spark.read.json(spark.sparkContext.parallelize([lineage_records_json]))
        domain_names = lineage_records_df.select('domain_name').distinct().rdd.flatMap(lambda x: x).collect() #.collect()
        final_rdd = spark.sparkContext.emptyRDD()
        final_cols = StructType([StructField('Full Name', StringType(), False),
                                 StructField('Name', StringType(), False),
                                 StructField('Community', StringType(), False),
                                 StructField('Domain Type', StringType(), False),
                                 StructField('Domain', StringType(), False),
                                 StructField('Asset Type', StringType(), False),
                                 StructField('Data Type', StringType(), False),
                                 StructField('Data Type Precision', StringType(), False),
                                 StructField('Size', StringType(), False),
                                 StructField('Data Classification', StringType(), False),
                                 StructField('File Location', StringType(), False),
                                 StructField('Format', StringType(), False),
                                 StructField('Description', StringType(), False),
                                 StructField('is part of [Table] > Name', StringType(), False),
                                 StructField('is part of [Table] > Full Name', StringType(), False),
                                 StructField('is part of [Table] > Community', StringType(), False),
                                 StructField('is part of [Table] > Domain', StringType(), False)])
        final_df = spark.createDataFrame(data=final_rdd,
                                         schema=final_cols)
        for domain_nm in domain_names:
            print(f'{SCRIPT_NAME} - {function_name} - Collibra registration for domain {domain_nm}')
            lineage_records_domain_df = lineage_records_df.filter(
                f"transformation_func_nm == 'Final' and domain_name == '{domain_nm}'")

            lineage_records_sel_cols_df = lineage_records_domain_df.select('domain_name', 'final_df_cols', 'target_tables')
            lineage_records_sel_table_df = lineage_records_domain_df.select('domain_name', 'target_tables','final_path')

            lineage_records_sel_table_df = lineage_records_sel_table_df.withColumn('domain_name',
                                                                                   when(F.col('domain_name') == 'esg',
                                                                                        'DataLake_ESG') \
                                                                                   .when(F.col('domain_name') == 'oem',
                                                                                         'DataLake_OEM') \
                                                                                   .when(F.col('domain_name') == 'external',
                                                                                         'DataLake_External') \
                                                                                   .when(F.col('domain_name') == 'finance',
                                                                                         'DataLake_Finance') \
                                                                                   .when(F.col('domain_name') == 'cross',
                                                                                         'DataLake_Cross') \
                                                                                   .when(
                                                                                       F.col('domain_name') == 'reference',
                                                                                       'DataLake_Reference') \
                                                                                   .when(F.col('domain_name') == 'ratings',
                                                                                         'DataLake_Ratings') \
                                                                                   .otherwise(F.col('domain_name')))

            #   inventory_records_df = inventory_records_df.withColumn('table_name'  , F.concat_ws('' , F.col('target_file_name') , F.col('target_table_name')))
            lineage_records_domain_df = lineage_records_sel_cols_df.withColumn('domain_name',
                                                                        when(F.col('domain_name') == 'esg', 'DataLake_ESG') \
                                                                        .when(F.col('domain_name') == 'oem', 'DataLake_OEM') \
                                                                        .when(F.col('domain_name') == 'external',
                                                                              'DataLake_External') \
                                                                        .when(F.col('domain_name') == 'finance',
                                                                              'DataLake_Finance') \
                                                                        .when(F.col('domain_name') == 'cross',
                                                                              'DataLake_Cross') \
                                                                        .when(F.col('domain_name') == 'reference',
                                                                              'DataLake_Reference') \
                                                                        .when(F.col('domain_name') == 'ratings',
                                                                              'DataLake_Ratings') \
                                                                        .otherwise(F.col('domain_name')))

            lineage_records_domain_df = lineage_records_domain_df.withColumn('columns', explode('final_df_cols'))

            lineage_records_domain_df = lineage_records_domain_df.withColumn('column_name',
                                                               split(lineage_records_domain_df['columns'], ', ').getItem(0)) \
                .withColumn('column_type', split(lineage_records_domain_df['columns'], ', ').getItem(1))

            lineage_records_domain_df = lineage_records_domain_df.select('domain_name', 'columns',
                                                           translate('column_name', "'( ", '').alias('col_name'),
                                                           translate('column_type', "') ", '').alias('col_type'),
                                                           'target_tables')

            lineage_records_domain_df = lineage_records_domain_df.withColumn('Full Name', F.concat_ws(' > ', lower(F.col('col_name')),
                                                                                        F.col('target_tables'))) \
                .withColumn('Name', lower(F.col('col_name'))) \
                .withColumn('Community', F.col('domain_name')) \
                .withColumn('Domain Type', F.lit('Physical Data Dictionary')) \
                .withColumn('Domain', F.lit('Processed')) \
                .withColumn('Asset Type', F.lit('Column')) \
                .withColumn('Data Type', when(F.col('col_type').startswith('decimal'),'decimal').otherwise(F.col('col_type'))) \
                .withColumn('Data Type Precision', F.split(F.regexp_replace(when(F.col('col_type').startswith('decimal'),F.split('col_type','decimal')[1]).otherwise(F.lit('')),"\\(",""),',')[1]) \
                .withColumn('Size', F.split(F.regexp_replace(when(F.col('col_type').startswith('decimal'),F.split('col_type','decimal')[1]).otherwise(F.lit('')),"\\(",""),',')[0]) \
                .withColumn('Data Classification', F.lit('')) \
                .withColumn('File Location', F.lit('')) \
                .withColumn('Format', F.lit('')) \
                .withColumn('Description', F.lit('')) \
                .withColumn('is part of [Table] > Name', F.col('target_tables')) \
                .withColumn('is part of [Table] > Full Name', F.concat_ws(' > ', F.col('col_name'), F.col('target_tables'))) \
                .withColumn('is part of [Table] > Community', F.col('domain_name')) \
                .withColumn('is part of [Table] > Domain', F.lit('Processed'))

            # lineage_table_df = lineage_records_domain_df.select('target_tables').distinct()
            lineage_table_df = lineage_records_sel_table_df.withColumn('Full Name', lower(F.col('target_tables'))) \
                .withColumn('Name', lower(F.col('target_tables'))) \
                .withColumn('Community', F.col('domain_name')) \
                .withColumn('Domain Type', F.lit('Physical Data Dictionary')) \
                .withColumn('Domain', F.lit('Processed')) \
                .withColumn('Asset Type', F.lit('Table')) \
                .withColumn('Data Type', F.lit('')) \
                .withColumn('Data Type Precision', F.lit('')) \
                .withColumn('Size', F.lit('')) \
                .withColumn('Data Classification', F.lit('')) \
                .withColumn('File Location', F.col('final_path')) \
                .withColumn('Format', F.lit('')) \
                .withColumn('Description', F.lit('')) \
                .withColumn('is part of [Table] > Name', F.lit('')) \
                .withColumn('is part of [Table] > Full Name', F.lit('')) \
                .withColumn('is part of [Table] > Community', F.lit('')) \
                .withColumn('is part of [Table] > Domain', F.lit(''))

            drop_table_tup = ('target_tables', 'domain_name','final_path')
            lineage_table_df = lineage_table_df.drop(*drop_table_tup)

            drop_cols_tup = ('columns', 'domain_name', 'col_name', 'col_type', 'target_tables')
            lineage_records_domain_df = lineage_records_domain_df.drop(*drop_cols_tup)

            lineage_records_domain_df = lineage_records_domain_df.withColumn('Data Type',
                                                               F.regexp_replace('Data Type', 'timestamp', 'Date Time')) \
                .withColumn('Data Type', F.regexp_replace('Data Type', 'string', 'Text')) \
                .withColumn('Data Type', F.regexp_replace('Data Type', 'int', 'Whole Number')) \
                .withColumn('Data Type', F.regexp_replace('Data Type', 'boolean', 'True/False')) \
                .withColumn('Data Type', F.regexp_replace('Data Type', 'datetime', 'Date Time')) \
                .withColumn('Data Type', F.regexp_replace('Data Type', 'bigDecimal Number', 'Decimal Number')) \
                .withColumn('Data Type', F.regexp_replace('Data Type', 'bigWhole Number', 'Whole Number')) \
                .withColumn('Data Type', F.regexp_replace('Data Type', 'date', 'Date')) \
                .withColumn('Data Type', F.regexp_replace('Data Type', 'decimal', 'Decimal Number'))
            collibra_csv_df = lineage_table_df.union(lineage_records_domain_df)
            final_df = final_df.union(collibra_csv_df)
        # write to /config/collibra location
        final_df.toPandas().to_csv(collibra_csv_output_path, header=True, index=False)
        return True

    except Exception as err:
        print(f'{SCRIPT_NAME} - {function_name} - Failed with error: {err}')
        raise Exception(f'{SCRIPT_NAME}  - Failed with error: {err}') from err

def load_collibra_processed(job_env, aws_region, collibra_token, job_monitoring_url, collibra_url):
    function_name = 'load_collibra_processed'
    aws_account_id = boto3.client(projectConstants.STS).get_caller_identity()[projectConstants.ACCOUNT]
    s3_csv_location = f"datamesh-ingfw-{aws_account_id}-{job_env}-{aws_region}"
    s3_csv_prefix = f"config/collibra/datamesh_collibra_processed.csv"
    generate_collibra_processed_csv_response = False
    generate_collibra_processed_csv_response = generate_collibra_processed_csv(job_env, s3_csv_location, s3_csv_prefix)
    if generate_collibra_processed_csv_response:
        collibra_job_id = upload_csv_to_collibra(collibra_url, collibra_token,s3_csv_location,s3_csv_prefix)
        print(f"{SCRIPT_NAME} - {function_name} - Collibra Response Job ID: {collibra_job_id}")
        print(f"{SCRIPT_NAME} - {function_name} - Waiting for 20 sec(s)")
        time.sleep(20)
        job_resp_json = get_collibra_job_details(job_monitoring_url, collibra_token, collibra_job_id)
        if job_resp_json['result'] == 'FAILURE':
            print(f"{SCRIPT_NAME} - {function_name} - Collibra Response : Failure| Message :{job_resp_json['message']}")
            raise SystemExit(
                f"{SCRIPT_NAME} - {function_name} - Collibra Response : Failure| Message :{job_resp_json['message']}")
        else:
            print(f"{SCRIPT_NAME} - {function_name} - {job_resp_json['message']}")

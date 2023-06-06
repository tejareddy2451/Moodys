import yaml, boto3, json
import numpy as np
from collections import defaultdict
import pyspark.sql.functions as F

from ingestion_framework.scripts import job_initializer
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants

# Module Constants
SCRIPT_NAME = "datamesh_parser"

def load_parser_util(job_env, aws_region):
    ''' Function reads the inventory table and creates app specific Yaml files '''
    function_name = 'load_parser_util'

    print(f'{SCRIPT_NAME} - {function_name} - processing to create the yaml file  has  started')
    sts_client      = boto3.client(projectConstants.STS)
    aws_account_id  = sts_client.get_caller_identity()[projectConstants.ACCOUNT]
    
    # Initilize spark
    spark = job_initializer.initializeSpark(projectConstants.PARSER)
    
    # Connect to dynamoDB
    print(f'{SCRIPT_NAME} - {function_name} - Connection to dynamo db  started')
    dynamodb = boto3.resource(dynamodbConstants.DYNAMO_DB, region_name=projectConstants.US_EAST_1)
    print(f'{SCRIPT_NAME} - Connection to dynamo db established :{dynamodb}')
    
    try:
        print(f'{SCRIPT_NAME} - Reading the Inventory Table in  :{dynamodb}  in environement {job_env}  started')
        inventory_table       = dynamodb.Table(dynamodbConstants.INV_TABLE.format(job_env))
        inventory_table_scan  = inventory_table.scan()
        inventory_table_items = inventory_table_scan[dynamodbConstants.TABLE_ITEMS]
        while dynamodbConstants.LAST_EVALUATED_KEY in inventory_table_scan:
            inventory_table_scan = inventory_table.scan(ExclusiveStartKey=inventory_table_scan[dynamodbConstants.LAST_EVALUATED_KEY])
            inventory_table_items.extend(inventory_table_scan[dynamodbConstants.TABLE_ITEMS])
        print(f'{SCRIPT_NAME} - Successfully read the Inventory Table {inventory_table} in  :{dynamodb}  in environement {job_env}  finished')

        print(f'{SCRIPT_NAME} - Reading -  cols_table in {dynamodb} to extract columns started ')
        cols_table       = dynamodb.Table(dynamodbConstants.COL_TABLE.format(job_env))
        cols_table_scan  = cols_table.scan()
        cols_table_items = cols_table_scan[dynamodbConstants.TABLE_ITEMS]
        while dynamodbConstants.LAST_EVALUATED_KEY in cols_table_scan:
            cols_table_scan = cols_table.scan(ExclusiveStartKey=cols_table_scan[dynamodbConstants.LAST_EVALUATED_KEY])
            cols_table_items.extend(cols_table_scan[dynamodbConstants.TABLE_ITEMS])
        print(f'{SCRIPT_NAME} - Successfully read the Columns Table -  :{cols_table} in {dynamodb} to extract columns finished ')

        # create encoder to account for non json serializable objects
        EncoderMethod = lambda self, obj: str(obj)
        EncoderMap    = {projectConstants.DEFAULT: EncoderMethod}
        Encoder       = type(projectConstants.ENCODER, (json.JSONEncoder,), EncoderMap)

        # generate json string of inventory table items and convert to Spark Dataframe
        print(f'{SCRIPT_NAME} - Conversion of inventory_records_json into a spark dataframe started')
        inventory_records_json = json.dumps(inventory_table_items, cls=Encoder)
        inventory_records_df   = spark.read.json(spark.sparkContext.parallelize([inventory_records_json]))
        print(f'{SCRIPT_NAME} - Conversion of inventory_records_json into a spark dataframe finished')

        print(f'{SCRIPT_NAME} - Conversion of col_records_json into a spark dataframe started')
        col_records_json = json.dumps(cols_table_items, cls=Encoder)
        col_records_df   = spark.read.json(spark.sparkContext.parallelize([col_records_json]))
        print(f'{SCRIPT_NAME} - Conversion of col_records_json into a spark dataframe finished')

        print(f'{SCRIPT_NAME} - Transforming col_records_df by grouping and aggregation max version number in dynamo for pipeline cols and create a new spark dataframe started')
        
        col_recs_grp_df = col_records_df.groupBy(dynamodbConstants.PIPELINE_ID, dynamodbConstants.VERSION_NUMBER) \
        .agg(F.collect_list(F.to_json(
        F.struct(dynamodbConstants.SEQUENCE_NUMBER, dynamodbConstants.SOURCE_COLUMN_NAME, dynamodbConstants.SOURCE_DATATYPE, \
                 dynamodbConstants.SOURCE_LENGTH, dynamodbConstants.SOURCE_PRECISION, dynamodbConstants.SOURCE_DATETIME_FORMAT, \
                 dynamodbConstants.TARGET_COLUMN_NAME, dynamodbConstants.TARGET_DATATYPE, dynamodbConstants.TARGET_LENGTH, \
                 dynamodbConstants.TARGET_PRECISION, dynamodbConstants.TARGET_DATETIME_FORMAT, dynamodbConstants.TRANSFORMATION_LOGIC))) \
             .alias(dynamodbConstants.PIPELINE_COLUMNS))
        col_recs_grp_df = col_recs_grp_df.withColumnRenamed(dynamodbConstants.PIPELINE_ID, projectConstants.P_ID)   \
            .withColumnRenamed(dynamodbConstants.VERSION_NUMBER, projectConstants.V_NO) \
                .select(projectConstants.P_ID, projectConstants.V_NO, F.concat(F.lit('['), F.concat_ws(',', dynamodbConstants.PIPELINE_COLUMNS), F.lit(']')).alias( dynamodbConstants.PIPELINE_COLUMNS))

        print(f'{SCRIPT_NAME} - Transforming col_records_df by grouping and aggregation to get max version number in dynamo for pipeline cols and create a new spark dataframe  finished')   
        
        
        # ensure that array/struct types have proper json format
        for column in inventory_records_df.dtypes:
            column_name  = column[0]
        column_data_type = column[1]
        if column_data_type != projectConstants.STRING:
            inventory_records_df = inventory_records_df.withColumn(column_name, F.to_json(column_name))
        
        print(f'{SCRIPT_NAME}- Converting spark dataframe inventory_records into a pandas dataframe started ')
        inventory_records_df = inventory_records_df.join(col_recs_grp_df,
                (inventory_records_df.pipeline_id    == col_recs_grp_df.p_id) &
                (inventory_records_df.version_number == col_recs_grp_df.v_no),
                projectConstants.INNER).drop(projectConstants.P_ID, projectConstants.V_NO)

        inventory_records_df = inventory_records_df.toPandas()
        print(f'{SCRIPT_NAME}- Converting spark dataframe inventory_records into a pandas dataframe finished ')

        # construct the dictionary with all apps
        print(f'{SCRIPT_NAME} - Constructing a dictionary with all apps started')
        columns        = inventory_records_df.columns
        app_name_index = columns.get_loc(dynamodbConstants.APP_NAME)
        app_groups     = defaultdict(lambda: [])

        for record in inventory_records_df.values:
            app_name = record[app_name_index]
            pipeline = np.delete(record, app_name_index)
            app_groups[app_name].append(pipeline)
        print(f'{SCRIPT_NAME} - Constructing a dictionary with all apps finished')

        # loop through each app and create app specific yaml files
        s3_client = boto3.client(projectConstants.S3)
        cols = list(columns.drop(dynamodbConstants.APP_NAME))
        for app_name, pipeline_data in app_groups.items():
            pipelines = [dict(zip(cols, data)) for data in pipeline_data]
            app_level_dictionary = {dynamodbConstants.APP_NAME: app_name, dynamodbConstants.PIPELINES: pipelines}
            datamesh_bucket_name = f"datamesh-ingfw-{aws_account_id}-{job_env}-{aws_region}"
            print(f'{SCRIPT_NAME} - {function_name} - processing to create the yaml file(s)  has  started')
            s3_client.put_object(Bucket=f"{datamesh_bucket_name}",Key=f"{projectConstants.DEFAULT_S3_YAML}{app_name}.{projectConstants.YAML}",
                Body=yaml.dump(app_level_dictionary))
        print(f'{SCRIPT_NAME} - {function_name} - processing to create the yaml file  has  completed')

    except Exception as err:
        print(f'{SCRIPT_NAME} - {function_name} -  Failed with error: {err}')
        raise Exception(f'{SCRIPT_NAME}  - Failed with error: {err}') from err

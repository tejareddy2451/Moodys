from datetime import datetime
from boto3.dynamodb.conditions import Attr
from pytz import timezone
import boto3
import json
import ast,re
from datetime import datetime, timedelta
from ingestion_framework.utils import logger_util
from ingestion_framework.utils import scd_util
from ingestion_framework.connector_framework.connector_utils import *
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants

now_timestamp = datetime.now(timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S")


#@logger_util.common_logger_exception
def get_source_query(spark, logger, inv_dict, pipeline_id, job_start_datetime, job_end_datetime, job_env):
    yaml = scd_util.dict2obj(inv_dict)
    query = yaml.source_query
    table = yaml.source_table_name
    fetch_from_time = ''
    fetch_till_time = ''

    if not query:
        print(f'Common Util - pipeline id:{pipeline_id} - Update Source Query - Started')

        #Forming the columns list
        columns_list = ast.literal_eval(yaml.pipeline_columns)
        sorted_columns_list = sorted(columns_list, key=lambda d: int(d[dynamodbConstants.SEQUENCE_NUMBER]))
        columns = ""
        for column in sorted_columns_list:
            columns += f"{column[dynamodbConstants.SOURCE_COLUMN_NAME]}, "
        columns = columns[:-2]

        #Forming the filter
        filter = ""
        if yaml.cdc_timestamp_key is not None and yaml.cdc_timestamp_key != [] and yaml.cdc_timestamp_key != '' and yaml.cdc_timestamp_key != 'null' :
            cdc_keys = ast.literal_eval(yaml.cdc_timestamp_key)
            job_status_table = f'datamesh-jobstatus-{job_env}'
            fetch_from_time = get_job_details(spark, job_status_table, pipeline_id) if job_start_datetime.strip() == '' else job_start_datetime
            if fetch_from_time == '':
                fetch_from_time = datetime(1900, 1, 1, 00, 00, 00, 00)
            fetch_till_time = now_timestamp if job_end_datetime.strip() == '' else job_end_datetime
            for cdc_key in cdc_keys :
                if yaml.source_database_type == projectConstants.ORACLE_DB:
                    filter += f"{cdc_key} at time zone 'America/New_York' between to_timestamp('{fetch_from_time}','yyyy-MM-dd hh24:mi:ss') \
                                and to_timestamp('{fetch_till_time}','yyyy-MM-dd hh24:mi:ss') or "
                elif yaml.source_database_type == projectConstants.POSTGRES_DB:
                    filter += f"{cdc_key} between at time zone 'America/New_York' '{fetch_from_time}' and '{fetch_till_time}' or "
                else:
                    filter += f"{cdc_key} between '{fetch_from_time}' and '{fetch_till_time}' or "
            filter = filter[:-4]
        if filter != "":
            filter = f" where {filter}"

        query = f"select {columns} from {table} {filter}"
        print(f'Common Util - pipeline id:{pipeline_id} - Updated Query - {query}')
        print(f'Common Util - pipeline id:{pipeline_id} - CDC Timestamp key - {yaml.cdc_timestamp_key}')
        print(f'Common Util - pipeline id:{pipeline_id} - Update Source Query - Finished')
    else :
        if '{}' in query:
            job_status_table = f'datamesh-jobstatus-{job_env}'            
            fetch_from_time = get_job_details(spark, job_status_table, pipeline_id) if job_start_datetime.strip() == '' else job_start_datetime
            fetch_till_time = now_timestamp if job_end_datetime.strip() == '' else job_end_datetime
            if fetch_from_time == '':
                fetch_from_time = datetime(1900, 1, 1, 00, 00, 00, 00)
                #Forming the columns list
                columns_list = ast.literal_eval(yaml.pipeline_columns)
                sorted_columns_list = sorted(columns_list, key=lambda d: int(d[dynamodbConstants.SEQUENCE_NUMBER]))
                columns = ""
                for column in sorted_columns_list:
                    columns += f"{column[dynamodbConstants.SOURCE_COLUMN_NAME]}, "
                columns = columns[:-2]

                query = f"select {columns} from {table}"
            else:
                query = query.format(fetch_from_time, fetch_till_time)
        
        print(f'Common Util - pipeline id:{pipeline_id} - Updated Query - {query}')
        
    return query, fetch_from_time, fetch_till_time

#@logger_util.common_logger_exception
def get_job_details(spark, job_status_table, pipeline_id):
    dynamodb = boto3.resource(dynamodbConstants.DYNAMO_DB, region_name=projectConstants.US_EAST_1)
    job_status_table = dynamodb.Table(job_status_table)

    job_status_table_scan = job_status_table.scan(
        ProjectionExpression='pipeline_id, job_status, execution_start_dttm',
        FilterExpression=Attr(dynamodbConstants.PIPELINE_ID).eq(pipeline_id)
    )
    job_status_table_items = job_status_table_scan[dynamodbConstants.TABLE_ITEMS]
    while dynamodbConstants.LAST_EVALUATED_KEY in job_status_table_scan:
        job_status_table_scan = job_status_table.scan(
            ExclusiveStartKey=job_status_table_scan[dynamodbConstants.LAST_EVALUATED_KEY],
            ProjectionExpression='pipeline_id, job_status, execution_start_dttm',
            FilterExpression=Attr(dynamodbConstants.PIPELINE_ID).eq(pipeline_id))
        job_status_table_items.extend(job_status_table_scan[dynamodbConstants.TABLE_ITEMS])

    # create encoder to account for non json serializable objects
    EncoderMethod = lambda self, obj: str(obj)
    EncoderMap = {projectConstants.DEFAULT: EncoderMethod}
    Encoder = type(projectConstants.ENCODER, (json.JSONEncoder,), EncoderMap)

    # generate json string of job status table items and convert to Spark Dataframe
    job_status_table_records_json = json.dumps(job_status_table_items, cls=Encoder)
    df = spark.read.json(spark.sparkContext.parallelize([job_status_table_records_json]))

    successful_run_df = df.filter("job_status = '{}'".format(projectConstants.SUCCESS_FLAG))
    #successful_run_df.orderBy(dynamodbConstants.EXECUTION_START_DTTM, ascending=[False]).show(50, False)
    last_successful_run_df = successful_run_df.orderBy(dynamodbConstants.EXECUTION_START_DTTM, ascending=[False]).limit(1)
    last_successful_run_df = last_successful_run_df.select(dynamodbConstants.EXECUTION_START_DTTM)
    #last_successful_run_df.show()
    last_run_time = last_successful_run_df.collect()[0][0] if last_successful_run_df.count() > 0 else ''
    return last_run_time

#@logger_util.common_logger_exception
def get_source_environment_details(src_config, job_env):
        src_env_details = dict()
        src_envs = src_config[dynamodbConstants.SOURCE_ENVIRONMENTS]
        src_envs = src_envs.replace('null','None')
        src_envs_list = ast.literal_eval(src_envs)
        for src_env_dict in src_envs_list:
            environment_name = src_env_dict[dynamodbConstants.ENVIRONMENT_NAME].lower()
            if job_env.lower() == environment_name:
                for k,v in src_env_dict.items():
                    src_env_details[k] = v
        return src_env_details

#@logger_util.common_logger_exception
def get_target_environment_details(tgt_config, job_env):
        tgt_env_details = dict()
        tgt_envs = tgt_config[dynamodbConstants.TARGET_ENVIRONMENTS]
        tgt_envs = tgt_envs.replace('null','None')
        tgt_envs_list = ast.literal_eval(tgt_envs)
        for tgt_env_dict in tgt_envs_list:
            environment_name = tgt_env_dict[dynamodbConstants.ENVIRONMENT_NAME].lower()
            if job_env.lower() == environment_name:
                for k,v in tgt_env_dict.items():
                    tgt_env_details[k] = v
        return tgt_env_details

#@logger_util.common_logger_exception
def get_sla_time_in_seconds(sla_time):
    h,m,s = sla_time.split(":")
    sla_time_in_seconds = int(h) * 3600 + int(m) * 60 + int(s)
    return sla_time_in_seconds

#@logger_util.common_logger_exception
def get_sla_met(job_start_time, job_end_time, inv_dict, logger):
    total_job_run_time = job_end_time - job_start_time
    total_job_run_time = total_job_run_time.total_seconds()

    sla_time = inv_dict[dynamodbConstants.SLA]
    if sla_time == "" or sla_time is None:
        return "not applicable"
    sla_time_in_seconds =  get_sla_time_in_seconds(sla_time)
    
    sla_met = sla_time_in_seconds > total_job_run_time

    if sla_met:
        print(f"Common Util - sla was met for the provided duration: {sla_time}")
        print(f"Common Util - the total job run time was {total_job_run_time/60} minutes")
        return "true"
    else:
        print(f"Common Util - sla was not met for the provided duration: {sla_time}")
        print(f"Common Util - the total job run time was {total_job_run_time/60} minutes")
        return "false"

#@logger_util.common_logger_exception
def get_current_timestamp(type):
    if type == 0:
        current_timestamp = str(datetime.now().timestamp()).replace(".", "")
    if type == 1:
        current_timestamp = datetime.now()
    return current_timestamp

#@logger_util.common_logger_exception
def get_current_date() :
    current_timestamp = datetime.now()
    current_date_MM_DD_YYYY = current_timestamp.strftime(projectConstants.DATE_FORMAT)
    return current_date_MM_DD_YYYY

#@logger_util.common_logger_exception
def pyspark_drop_NA_columns(df, how='any'):
    columnsToDrop = []
    numRows = df.count()
    if how=='all':
        for col in df.columns:
            withoutnulls = df.select(col).dropna()
            if withoutnulls.count()==0: #all values in column were null
                columnsToDrop.append(col)
    else:
        for col in df.columns:
            withoutnulls = df.select(col).dropna()
            if withoutnulls.count() < numRows:
                columnsToDrop.append(col)
    return df.drop(*columnsToDrop)

#@logger_util.common_logger_exception
def get_spark_df_from_query(db_type, secret, query, spark, logger, job_env):
    config_dict = {
        dynamodbConstants.SOURCE_TYPE : dynamodbConstants.DATABASE, 
        dynamodbConstants.SOURCE_DATABASE_TYPE : db_type, 
        dynamodbConstants.SOURCE_ENVIRONMENTS : '[{{"{}": "{}", "{}": "{}"}}]'.format(dynamodbConstants.ENVIRONMENT_NAME, job_env, dynamodbConstants.DATABASE_SECRET_MANAGER_NAME, secret), 
        dynamodbConstants.SOURCE_QUERY : query
        }
    src_dict = ConnectorConfig(config_dict[dynamodbConstants.SOURCE_DATABASE_TYPE], config_dict)
    src_connector = ConnectorSupplier(spark, logger, job_env).get_connector(src_dict)
    df = src_connector.read_data()
    return df

#@logger_util.common_logger_exception
def write_spark_df_to_db(df, target_type, secret, target_name, spark, logger, job_env):
    if target_type == projectConstants.S3:
        config_dict = {
            dynamodbConstants.TARGET_TYPE : target_type,
            dynamodbConstants.TARGET_ENVIRONMENTS : '[{{"{}": "{}", "{}": "{}"}}]'.format(dynamodbConstants.ENVIRONMENT_NAME, job_env, dynamodbConstants.DATABASE_SECRET_MANAGER_NAME, secret),
            dynamodbConstants.TARGET_FILE_NAME : target_name
        }
    else :
        config_dict = {
            dynamodbConstants.TARGET_TYPE : target_type,
            dynamodbConstants.TARGET_ENVIRONMENTS : '[{{"{}": "{}", "{}": "{}"}}]'.format(dynamodbConstants.ENVIRONMENT_NAME, job_env, dynamodbConstants.DATABASE_SECRET_MANAGER_NAME, secret),
            dynamodbConstants.TARGET_TABLE_NAME : target_name
        }
    tgt_dict = ConnectorConfig(config_dict[dynamodbConstants.TARGET_TYPE], config_dict)
    tgt_connector = ConnectorSupplier(spark, logger, job_env).get_connector(tgt_dict)
    tgt_connector.write_data(df)
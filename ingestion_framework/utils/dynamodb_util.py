import boto3
from boto3.dynamodb.conditions import Key
from awsglue.utils import getResolvedOptions
from datetime import datetime, timedelta
from pytz import timezone

from ingestion_framework.utils import common_util
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants


dynamodb = boto3.resource(dynamodbConstants.DYNAMO_DB, region_name=projectConstants.US_EAST_1)
class JobStatus:
    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger


    def update_jobstatus(self, inv_dict, job_env ,pipeline_id, exec_id, business_order_from_dttm, business_order_to_dttm, execution_start_dttm, execution_end_dttm, flag, error_msg, lnd_cnt, fnl_cnt, processed_cnt, sla_met):
        dynamo_table_name = dynamodbConstants.JOB_STATUS_TABLE.format(job_env)
        #dynamodb = boto3.resource(dynamodbConstants.DYNAMO_DB, region_name=projectConstants.US_EAST_1)
        table = dynamodb.Table(dynamo_table_name)
        response = table.update_item(
                Key={
                    dynamodbConstants.PIPELINE_ID: pipeline_id,
                    dynamodbConstants.EXECUTION_ID: exec_id
                },
                UpdateExpression="set job_status=:flag_new, error_message=:error_msg_new, \
                                    landing_count=:lnd_cnt_new, final_count=:fnl_cnt_new, processed_count=:processed_cnt_new, \
                                    sla_met=:sla_met_new, \
                                    business_order_from_dttm=:business_order_from_dttm_new, \
                                    business_order_to_dttm=:business_order_to_dttm_new, \
                                    execution_start_dttm=:execution_start_dttm_new, \
                                    execution_end_dttm=:execution_end_dttm_new",
                ExpressionAttributeValues={
                    ':flag_new': flag,
                    ':error_msg_new': error_msg,
                    ':lnd_cnt_new': lnd_cnt,
                    ':fnl_cnt_new': fnl_cnt,
                    ':processed_cnt_new': processed_cnt,
                    ':sla_met_new': sla_met,
                    ':business_order_from_dttm_new': business_order_from_dttm,
                    ':business_order_to_dttm_new': business_order_to_dttm,
                    ':execution_start_dttm_new': execution_start_dttm,
                    ':execution_end_dttm_new': execution_end_dttm
                },
                ReturnValues="UPDATED_NEW"
        )
        return response

    #Add a record into jobstatus DynamoDB table
    def put_jobstatus(self, inv_dict, job_start_time, job_env):
        dynamo_table_name = dynamodbConstants.JOB_STATUS_TABLE.format(job_env)
        item_structure = self.get_job_status_item_structure(inv_dict, job_start_time, job_env)
        #dynamodb = boto3.resource(dynamodbConstants.DYNAMO_DB, region_name=projectConstants.US_EAST_1)
        table = dynamodb.Table(dynamo_table_name)
        response = table.put_item(
                Item=item_structure
        )
        return item_structure[dynamodbConstants.EXECUTION_ID]

    #Get a record from jobstatus DynamoDB table
    def query_jobstatus(self, dynamo_table_name, pipeline_id):
        #dynamodb = boto3.resource(dynamodbConstants.DYNAMO_DB, region_name=projectConstants.US_EAST_1)
        table = dynamodb.Table(dynamo_table_name)
        response = table.scan(
                FilterExpression=Key(dynamodbConstants.PIPELINE_ID).eq(pipeline_id)
        )
        response_items = response['Items']
        while dynamodbConstants.LAST_EVALUATED_KEY in response:
            response = table.scan(FilterExpression=Key(dynamodbConstants.PIPELINE_ID).eq(pipeline_id),ExclusiveStartKey=response['LastEvaluatedKey'])
            response_items.extend(response['Items'])
        return response_items

     #Bulk delete records from DynamoDB table
    def bulk_delete_dynamodb(self, table_name, sort_key, partition_key, sk_values):
        
        #1. Table Name is Mandatory
        if not table_name:
            raise SystemExit('Table Name is needed')
    
        #2. Sort Key is Mandatory
        if not sort_key:
            raise SystemExit('Sort Key is needed')

        #3. Partition Key is Mandatory
        if not partition_key:
            raise SystemExit('Partition Key is needed')

        #4. Sort Key values are Mandatory
        if not sk_values:
            raise SystemExit('Sort Key Values are needed')

        try:
            print("Got all the values")
            print(f"tableName : {table_name}")
            print(f"patitionKey : {partition_key}")
            print(f"sortKey : {sort_key}")
            print(f"sortKeyValues : {sk_values}")
            ddb_table = dynamodb.Table(table_name)
            sk_values_list = sk_values.split(',')
            for sk_id in sk_values_list:
                ddb_table_scan = ddb_table.scan(FilterExpression=Key(sort_key).eq(sk_id))
                ddb_table_items = ddb_table_scan[dynamodbConstants.TABLE_ITEMS]
                
                while dynamodbConstants.LAST_EVALUATED_KEY in ddb_table_scan:
                    ddb_table_scan = ddb_table.scan(FilterExpression=Key(sort_key).eq(sk_id),ExclusiveStartKey=ddb_table_scan[dynamodbConstants.LAST_EVALUATED_KEY])
                    ddb_table_items.extend(ddb_table_scan[dynamodbConstants.TABLE_ITEMS])
                 
                print(f"Total records identified : {len(ddb_table_items)}")
                try:
                    for item in ddb_table_items:
                        response = ddb_table.delete_item(Key={sort_key: item[sort_key], partition_key: item[partition_key]})
                        status_code = response[dynamodbConstants.RESPONSE_METADATA][dynamodbConstants.HTTP_STATUS_CODE]
                        if status_code != 200:
                            raise ValueError(f'Failed Item deletion - {sort_key},{partition_key} : {item[sort_key]}, {item[partition_key]}')
                except ValueError as error:
                    pass
            print("Finished")
        except Exception as err:
            raise Exception(f'Failed with error: {err}')


    def get_job_status_item_structure(self, inv_dict, job_start_time, env):
        item_structure={}
        item_structure[dynamodbConstants.PIPELINE_ID] = inv_dict[dynamodbConstants.PIPELINE_ID]
  
        if "load_phase" not in inv_dict:
            load_phase = "raw"
        else:
            load_phase = inv_dict[dynamodbConstants.LOAD_PHASE]
        domain_name = inv_dict[dynamodbConstants.DOMAIN_NAME]
        
        if inv_dict[dynamodbConstants.TARGET_TYPE] == projectConstants.S3:
            target_name = inv_dict[dynamodbConstants.TARGET_FILE_NAME]
            table_name = f"datamesh_{domain_name}_{load_phase}_{env}.{target_name}"
        else:
            table_name = inv_dict[dynamodbConstants.TARGET_TABLE_NAME]
        
        

        item_structure[dynamodbConstants.TABLE_NAME] = table_name
        item_structure[dynamodbConstants.EXECUTION_ID]    = common_util.get_current_timestamp(0)
        item_structure[dynamodbConstants.EXECUTION_START_DTTM] = job_start_time
        item_structure[dynamodbConstants.EXECUTION_END_DTTM] = projectConstants.NONE
        item_structure[dynamodbConstants.BUSINESS_ORDER_FROM_DTTM] = projectConstants.NONE
        item_structure[dynamodbConstants.BUSINESS_ORDER_TO_DTTM] = projectConstants.NONE
        item_structure[dynamodbConstants.LOAD_TYPE] = inv_dict["load_type"]
        item_structure[dynamodbConstants.LOAD_PHASE] = load_phase
        item_structure[dynamodbConstants.FREQUENCY_TYPE] = inv_dict["load_frequency"]
        item_structure[dynamodbConstants.LANDING_COUNT] = 0
        item_structure[dynamodbConstants.FINAL_COUNT] = 0
        item_structure[dynamodbConstants.PROCESSED_COUNT] = 0
        item_structure[dynamodbConstants.ERROR_MESSAGE] = projectConstants.NONE
        item_structure[dynamodbConstants.JOB_STATUS] = projectConstants.STARTED_FLAG
        item_structure[dynamodbConstants.SLA_MET] = projectConstants.NONE
        return item_structure
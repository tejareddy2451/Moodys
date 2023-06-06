import boto3
import datetime
import re

from ingestion_framework.connector_framework.connector_utils import *
from ingestion_framework.utils import common_util
from ingestion_framework.utils import logger_util
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants


from botocore.exceptions import ClientError
    
#@logger_util.common_logger_exception
def get_s3_file_path(bucket_name, dbtype, table_name, process_date):
    return f"s3://{bucket_name}/data/sbi/final/{dbtype}/{table_name}/LOADDTPTN={process_date}"

#@logger_util.common_logger_exception
def check_s3_file_exists(bucketName, filePath):
    s3 = boto3.resource(projectConstants.S3)
    bucket = s3.Bucket(bucketName)
    result = bucket.meta.client.list_objects(Bucket=bucket.name,
                                            Prefix=filePath ,
                                            Delimiter='/')
    exists = True
    if result.get('CommonPrefixes') is None and result.get('Contents') is None:
        exists = False
    return exists

#@logger_util.common_logger_exception
def delete_s3_files(bucketName, filePath):
    #from boto.s3.key import Key
    s3 = boto3.resource(projectConstants.S3)
    bucket = s3.Bucket(bucketName)
    #fileKey = Key(bucket, filePath)
    #fileKey.delete()
    for key in bucket.objects.all():
        if key.key in filePath:
            key.delete()
    return "done"

#@logger_util.common_logger_exception
def get_last_reconciled_file_date(bucketName, rootPath, dataset, tableName):
    s3 = boto3.resource(projectConstants.S3)
    bucket = s3.Bucket(bucketName)
    keyPath = rootPath +  dataset + '/Full/' +  dataset + '_' + tableName + '_'
    #printToConsole('Checking...' +  keyPath)
    if not check_s3_file_exists(bucketName, keyPath):
        return None
    result = bucket.meta.client.list_objects(Bucket=bucket.name,
        Prefix=keyPath,
        Delimiter='/')
    allFiles = result.get('Contents')
    files = {}
    for file in allFiles:
        filedate = re.search("(\d{8})" , file.get('Key') )
        #if (file.get('Key').endswith(tableName + "_" + filedate.group(0) + "_reconciled.csv")) :
        #if (file.get('Key').find('_' + tableName + '_' + filedate.group(0))):
        if (re.search('_' + tableName + '_' + filedate.group(0), file.get('Key'))):
            filedate = datetime.datetime.strptime(filedate.group(0), "%Y%m%d").date()
            files[file.get('Key')] = filedate
    if len(files) == 0:
        return None
    #sort the file dictionary by date descending
    sortedlist = sorted(files.items(), key=lambda x: x[1], reverse=True)
    lastReconciledFile = sortedlist[0][1]
    return lastReconciledFile.strftime('%Y%m%d')
    

#@logger_util.common_logger_exception
def s3_filesize_in_MB(bucket, key):
    #printToConsole('Begin S3 filesize check')
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=key,
    )
    for obj in response.get('Contents', []):
        if obj['Key'] == key:
            filesize = float(obj['Size'])/(1024*1024)
    #printToConsole('End S3 filesize check')
    return filesize

#@logger_util.common_logger_exception
def set_s3_lifecycle_policy(bucket_name, table_name, trn_days, expr_days, status, storage_class):
    client = boto3.client(projectConstants.S3)
    try :
        current_lc_conf = client.get_bucket_lifecycle_configuration( Bucket=bucket_name )
        print(f"S3 Util - Lifecycle policy has already been created for this bucket. Adding a new rule for table {table_name}")
        new_rule_name = f'lifecycle_{table_name}'
        new_rule={
                    'ID': new_rule_name,
                    'Filter': { 'Prefix': f'data/landing/{table_name}/' },
                    'Transitions': [ { 'Days': trn_days, 'StorageClass': storage_class } ],                    
                    'Expiration': { 'Days': expr_days },
                    'AbortIncompleteMultipartUpload': { 'DaysAfterInitiation': expr_days },
                    'Status': status
                }
        rules = current_lc_conf['Rules']
        if any(rule['ID'] == new_rule_name for rule in rules):
            print(f"S3 Util - Rule {new_rule_name} already exists. Updating rule with new attributes.")
            # get index of existing rule from list of rules
            existing_rule_index = next(i for i,rule in enumerate(rules) if rule['ID'] == new_rule_name)
            del rules[existing_rule_index]
        
        rules.append(new_rule)
        # Update bucket policy with new rule details        
        response = client.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration={
                'Rules': rules
            }
        )
        print(f"S3 Util - Updated policy for bucket with table : {table_name}")
    except ClientError as e:
        if 'NoSuchLifecycleConfiguration' in e.__str__() :
            print(f"S3 Util - No lifecycle policy created yet for this bucket. Creating the new one with table {table_name}")
            response = client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration={
                    'Rules': [
                        {
                            'ID': f'lifecycle_{table_name}',
                            'Filter': { 'Prefix': f'data/landing/{table_name}/' },
                            'Transitions': [ { 'Days': trn_days, 'StorageClass': storage_class } ],                    
                            'Expiration': { 'Days': expr_days },
                            'AbortIncompleteMultipartUpload': { 'DaysAfterInitiation': expr_days },
                            'Status': status
                        }
                    ]
                }
            )
            print(f"S3 Util - Created policy for bucket with table : {table_name}")
    except Exception as e:
        raise Exception( "Unexpected error in get_bucket_lifecycle_of_s3 function: " + e.__str__())


@logger_util.common_logger_exception
def update_s3_life_cycle_policy(tgt_config, job_env):
    target_details = common_util.get_target_environment_details(tgt_config, job_env)
    landing_standard_retention_days = 0 #Since we are not keeping landing files in standard class
    landing_glacier_retention_days = int(target_details[dynamodbConstants.LANDING_GLACIER_RETENTION_DAYS])
    set_s3_lifecycle_policy(target_details[dynamodbConstants.TARGET_BUCKET_NAME], 
    tgt_config[dynamodbConstants.TARGET_FILE_NAME],
    landing_standard_retention_days, landing_glacier_retention_days, 'Enabled', 'GLACIER')

"""
@logger_util.common_logger_exception
def get_spark_df_from_s3(source_bucket, target_name, spark, logger, job_env):
    config_dict = {
        "source_type": 's3', 
        "source_environments": '[{{"environment_name": "{}", "source_bucket_name": "{}"}}]'.format(job_env, source_bucket), 
        "target_name": target_name,
        }
    src_dict = ConnectorConfig(config_dict['source_type'], config_dict)
    src_connector = ConnectorSupplier(spark, logger, job_env).get_connector(src_dict)
    df = src_connector.read_data()
    return df

@logger_util.common_logger_exception
def write_spark_df_to_s3(df, target_bucket, target_name, spark, logger, job_env):
    config_dict = {
        "target_type": 's3',
        "target_environments": '[{{"environment_name": "{}", "target_bucket_name": "{}"}}]'.format(job_env, target_bucket),
        "target_name": target_name,
        "database_type": ''
    }
    tgt_dict = ConnectorConfig(config_dict['target_type'], config_dict)
    tgt_connector = ConnectorSupplier(spark, logger, job_env, 'write').get_connector(tgt_dict)
    tgt_connector.write_data(df)
"""
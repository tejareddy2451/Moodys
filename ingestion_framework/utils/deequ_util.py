"""
Module :Deequ Logic 
"""

import pydeequ
from pydeequ.analyzers import *
from pydeequ.suggestions import *
from pydeequ.verification import *
from pydeequ.checks import *
from pydeequ.profiles import *
import boto3
from boto3.dynamodb.conditions import Key
import numpy as np
import datetime
from datetime import date
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants
from ingestion_framework.connector_framework.connector_utils import *
from ingestion_framework.utils import *
from ingestion_framework.scripts import job_initializer
from ingestion_framework.utils import logger_util
from pyspark.sql import SparkSession
import ast


#@logger_util.common_logger_exception
def deequ_suggestion_and_validation(tgt_df, spark, dynamodb, env, inv_id):
    execution_time = datetime.now()
    tbl_dq_sug = dynamodb.Table(dynamodbConstants.DQ_SUGGESTION_TABLE.format(env))
    tbl_dq_res = dynamodb.Table(dynamodbConstants.DQ_TRACKER_TABLE.format(env))

    # Query and delete the old records.
    del_items = tbl_dq_sug.query(
        KeyConditionExpression=Key(dynamodbConstants.PIPELINE_ID).eq(inv_id), IndexName='pipeline_id-index'
    )[dynamodbConstants.TABLE_ITEMS]

    # Delete
    for del_item in del_items:
        tbl_dq_sug.delete_item(
            Key={
                dynamodbConstants.EXECUTION_ID: del_item[dynamodbConstants.EXECUTION_ID],
                dynamodbConstants.PIPELINE_ID: del_item[dynamodbConstants.PIPELINE_ID]
            }
        )

    tbl_inv_cols = dynamodb.Table(dynamodbConstants.COL_TABLE.format(env))
    cols = tbl_inv_cols.query(
        KeyConditionExpression=Key(dynamodbConstants.PIPELINE_ID).eq(inv_id), IndexName='pipelineid-gsi-index'
    )[dynamodbConstants.TABLE_ITEMS]
    dq_str = ""
    for col in cols:
        src_col = col['source_column_name']
        if 'dq_rule_set' in col.keys():
            for rule in ast.literal_eval(col['dq_rule_set']):
                dq_str = dq_str + "." + rule['dq_rule'].replace("()", "(\"" + src_col + "\")")

    if dq_str == "":
        sug_res_tgt = (
            ConstraintSuggestionRunner(spark)
                .onData(tgt_df)
#                .addConstraintRule(DEFAULT())
                .addConstraintRule(CompleteIfCompleteRule())
                .addConstraintRule(NonNegativeNumbersRule())
                .addConstraintRule(RetainCompletenessRule())
                .addConstraintRule(RetainTypeRule())
                .addConstraintRule(UniqueIfApproximatelyUniqueRule())
                .addConstraintRule(CategoricalRangeRule())
                .addConstraintRule(FractionalCategoricalRangeRule())
                .run()
        )

        sug_lst_tgt = sug_res_tgt.get("constraint_suggestions")
        for sug in sug_lst_tgt:
            execution_start_time = datetime.now()
            ts = execution_start_time.timestamp()
            curr_dt = date.today()
            execution_end_time = datetime.now()
            if ".isContainedIn(" not in sug.get('code_for_constraint'):
                tbl_dq_sug.put_item(
                    Item={
                        dynamodbConstants.EXECUTION_ID: f"{inv_id}_{str(ts)}",
                        dynamodbConstants.PIPELINE_ID: inv_id,
                        'source_col_name': sug.get('column_name'),
                        'dq_suggestion_result': sug.get('description'),
                        dynamodbConstants.EXECUTION_START_DTTM: str(execution_start_time),
                        dynamodbConstants.EXECUTION_END_DTTM: str(execution_end_time),
                        dynamodbConstants.BUSINESS_ORDER_FROM_DTTM: str(curr_dt),
                        dynamodbConstants.BUSINESS_ORDER_TO_DTTM: str(execution_end_time),
                        'dq_rule': sug.get('code_for_constraint')
                    }
                )
    for col in cols:
        src_col = col['source_column_name']
        if 'dq_rule_set' in col.keys():
            for rule in ast.literal_eval(col['dq_rule_set']):
                dq_str = ""
                dq_str = dq_str + "." + rule['dq_rule'].replace("()", "(\"" + src_col + "\")")
                check = Check(spark, CheckLevel.Warning, "Review Check")
                check_str_tgt = "VerificationSuite(spark).onData(tgt_df).addCheck(check" + dq_str + ").run()"
                check_run_tgt = eval(check_str_tgt)

                # Constraint check
                check_res_tgt_df_c = VerificationResult.checkResultsAsDataFrame(spark, check_run_tgt)
                check_res_tgt_df_pd = check_res_tgt_df_c.toPandas()

                for index, row in check_res_tgt_df_pd.iterrows():
                    execution_start_time = datetime.now()
                    ts = execution_start_time.timestamp()
                    tbl_dq_res.put_item(
                        Item={
                            dynamodbConstants.EXECUTION_ID: str(f"{inv_id}_{ts}"),
                            dynamodbConstants.PIPELINE_ID: inv_id,
                            'job_execution_time': str(execution_time),
                            'column_name': src_col,
                            'dq_rule': dq_str,
                            'dq_rule_description': rule['dq_desc'],
                            'constraint': row['constraint'],
                            'constraint_status': row['constraint_status'],
                            'constraint_message': row['constraint_message']
                        }
                    )


#@logger_util.common_logger_exception
def deequ_validation(env, app_name):
    failed_pipelines = []

    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", pydeequ.deequ_maven_coord) \
        .config("spark.jars.excludes", pydeequ.f2j_maven_coord) \
        .getOrCreate()

    logger = job_initializer.getGlueLogger(spark)
    dynamodb = boto3.resource(dynamodbConstants.DYNAMO_DB, region_name=projectConstants.US_EAST_1)

    pipelines_with_versions = InventoryReader.get_all_pipelineids_with_latest_versions(app_name)
    print('Deequ Util - List of pipelines with versions identified:')
    for p_id,v in pipelines_with_versions.items():
        print(f'Deequ Util - Pipeline Id:{p_id}  -> Version:{v}')
    #pls = list({pl[dynamodbConstants.PIPELINE_ID]: pl for pl in pipelines}.values())
    for pipeline_id, version in pipelines_with_versions.items():
        try:
            print(f'Deequ Util - {pipeline_id} - Started')
                
            inv_dict = InventoryReader.get_pipeline_dict_from_yaml(app_name, pipeline_id, version)
            print(f'Deequ Util - {pipeline_id} - Got the inventory dictionary')
                
            tgt_dict = InventoryReader(inv_dict).get_target_connector_config()
            print(f'Deequ Util - {pipeline_id} - Got the target dictionary')
            
            target_details = common_util.get_target_environment_details(inv_dict, env)
            print(f'Deequ Util - {pipeline_id} - Got the target details')
            tgt_bucket = target_details[dynamodbConstants.TARGET_BUCKET_NAME]
            tgt_name = inv_dict[dynamodbConstants.TARGET_FILE_NAME]
            print(f'Deequ Util - {pipeline_id} - Got the target bucket name and file name')
                
            df_tgt = spark.read.parquet(f"s3a://{tgt_bucket}/data/final/{tgt_name}/")
            print(f'Deequ Util - {pipeline_id} - read the data')
                
            print(f'Deequ Util - {pipeline_id} - DQ check - Started')
            deequ_suggestion_and_validation(df_tgt, spark, dynamodb, env, inv_dict[dynamodbConstants.PIPELINE_ID])
            print(f'Deequ Util - {pipeline_id} - DQ check - Completed')

            print(f'Deequ Util - {pipeline_id} - Completed')
        except Exception as err:
            print(f'Deequ Util - {pipeline_id} - Failed with error: {err}')
            failed_pipelines.append(pipeline_id)
    
    print(f'Deequ Util - Clean up - Started')
    spark.sparkContext._gateway.shutdown_callback_server()
    spark.stop()
    print(f'Deequ Util - Clean up - Completed')

    return failed_pipelines
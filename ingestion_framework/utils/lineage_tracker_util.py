import boto3
from datetime import datetime
from botocore.exceptions import ClientError
from dataclasses import dataclass, fields, field
from typing import Any
from ingestion_framework.constants import dynamodbConstants, projectConstants
from boto3.dynamodb.conditions import Key

@dataclass
class DynamoRecord:
    """ Represents the schema of our records stored in DynamoDB """
    script_name: str
    exec_ts: str
    domain_name: str
    transformation_func_nm: str = ''
    added_col: list = field(default_factory=list)
    selected_cols: list = field(default_factory=list)
    renamed_cols: list = field(default_factory=list)
    dropped_cols: list = field(default_factory=list)
    left_df_cols: list = field(default_factory=list)
    left_df_id: str = ''
    right_df_cols: list = field(default_factory=list)
    right_df_id: str = ''
    key_cols: list = field(default_factory=list)
    left_key_cols: list = field(default_factory=list)
    right_key_cols: list = field(default_factory=list)
    final_df_cols: list = field(default_factory=list)
    source_tables: str = ''
    target_tables: str = ''
    sql_text: str = ''
    transformed_df = ''
    final_path = ''
    _update_expression = 'SET '
    _expression_attribute_values = {}

    def insert_to_table(self, env):
        print(f'Lineage Tracker Util - insert_to_table - insert into tracker table {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        dynamodb = boto3.resource(dynamodbConstants.DYNAMO_DB)
        dynamo_table = dynamodb.Table(dynamodbConstants.LINEAGE_TRACKER_TABLE.format(env))
        try:
            dynamo_table.put_item(Item={
                'script_name': self.script_name,
                'exec_ts': str(datetime.now()),
                'domain_name' : self.domain_name,
                'transformation_func_nm': self.transformation_func_nm,
                'added_col': self.added_col,
                'selected_cols': self.selected_cols,
                'renamed_cols': self.renamed_cols,
                'dropped_cols': self.dropped_cols,
                'left_df_cols': self.left_df_cols,
                'left_df_id': self.left_df_id,
                'right_df_cols': self.right_df_cols,
                'right_df_id': self.right_df_id,
                'key_cols': self.key_cols,
                'left_key_cols': self.left_key_cols,
                'right_key_cols': self.right_key_cols,
                'final_df_cols': self.final_df_cols,
                'target_tables': self.target_tables,
                'source_tables': self.source_tables,
                'sql_text': self.sql_text,
                'transformed_df': self.transformed_df,
                'final_path': self.final_path
            })
        except Exception as e:
            print(f'Lineage Tracker Util - insert_to_table - Error inserting {self.script_name}')
            print(self)
            raise e

def sources_hierarchy(src_tgt_dict, tgt_df_id):
    print(
        f'Lineage Tracker Util - sources_hierarchy - align sources hierarchy - Started')
    src_keys = src_tgt_dict.keys()
    final_src_list = []
    if tgt_df_id in src_keys:
        tmp_src_list = src_tgt_dict[tgt_df_id]
        while tmp_src_list:
            recheck_src = []
            for val in tmp_src_list:
                if val in src_keys:
                    if isinstance(val, str):
                        final_src_list.append(val)
                    elif isinstance(val, int):
                        recheck_src.extend(src_tgt_dict[val])
                elif isinstance(val, str):
                    final_src_list.append(val)
            tmp_src_list = recheck_src
    print(
        f'Lineage Tracker Util - sources_hierarchy - align sources hierarchy - Finished')
    return final_src_list

def insert_dynamo_record(glue_job_nm, transf_func_nm, env, src_tgt_mapping, domain_name, transf_func_args):
    try:
        print(f'Lineage Tracker Util - insert_dynamo_record - Job Name:{glue_job_nm} - Insert into tracker table - Started')
        lineage_dynamo_record = DynamoRecord(glue_job_nm, str(datetime.now()), domain_name)
        lineage_dynamo_record.transformation_func_nm = transf_func_nm
        for col_name, value in transf_func_args.items():
            if col_name == "source_tables":
                continue
            if col_name == 'target_tables':
                src_tgt_mapping[value] = transf_func_args["source_tables"] if isinstance(transf_func_args["source_tables"], list) else transf_func_args["source_tables"].split(',')
                final_src_list = sources_hierarchy(src_tgt_mapping, value)
                lineage_dynamo_record.source_tables = final_src_list
                lineage_dynamo_record.transformed_df = transf_func_args["source_tables"]
            setattr(lineage_dynamo_record, col_name, value)
        ins_response = lineage_dynamo_record.insert_to_table(env)
        print(f'Lineage Tracker Util - insert_dynamo_record - Job Name:{glue_job_nm} - Insert into tracker table - Finished')
        return
    except Exception as exc:
        print(f'Lineage Tracker Util - insert_dynamo_record - Job Name:{glue_job_nm} - Insert into tracker table - Failed with error:{exc}')
        raise exc

def bulk_delete_dynamo_records(glue_job_nm, env):
    print(f'Lineage Tracker Util - bulk_delete_dynamo_records - Job Name:{glue_job_nm} - Delete from tracker table - Started')
    dynamodb = boto3.resource(dynamodbConstants.DYNAMO_DB)
    dynamo_table = dynamodb.Table(dynamodbConstants.LINEAGE_TRACKER_TABLE.format(env))

    ddb_table_scan = dynamo_table.query(
        KeyConditionExpression=Key('script_name').eq(glue_job_nm)
    )
    ddb_table_items = ddb_table_scan[dynamodbConstants.TABLE_ITEMS]

    try:
        for item in ddb_table_items:
            response = dynamo_table.delete_item(Key={'script_name': item['script_name'], 'exec_ts':item['exec_ts']})
            status_code = response[dynamodbConstants.RESPONSE_METADATA][dynamodbConstants.HTTP_STATUS_CODE]
            if status_code != int(projectConstants.HTTP_SUCCESS):
                print(f"Lineage Tracker Util - bulk_delete_dynamo_records - delete_item response - {response[dynamodbConstants.RESPONSE_METADATA]}")
                raise ValueError(
                    f"Lineage Tracker Util - bulk_delete_dynamo_records - Failed Item deletion - 'script_name' : {item['script_name']}")
        print(
            f'Lineage Tracker Util - bulk_delete_dynamo_records - Job Name:{glue_job_nm} - Delete from tracker table - Finished')
    except ValueError as error:
        print(f'Lineage Tracker Util - bulk_delete_dynamo_records - Cleanup in progress, but found error: {error}')
        raise error
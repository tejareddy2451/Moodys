import boto3
from data_quality_framework.constants import bdqConstants

def get_tgt_db_table_name_athena(db_table_name):
    if "." not in db_table_name:
        return False, False
    db_name, table_name = db_table_name.split(".")
    return db_name, table_name

def get_tgt_db_table_name_postgres(db_schema_table_name):
    if "." not in db_schema_table_name:
        return False, False, False
    if len(db_schema_table_name.split(".")) < 3:
        return False, False, False
    db_name, schema_name, table_name = db_schema_table_name.split(".")
    return db_name, schema_name, table_name

def get_account_id():
    sts_client = boto3.client(bdqConstants.STS)
    aws_account_id = sts_client.get_caller_identity()[bdqConstants.ACCOUNT]
    return aws_account_id

def get_region():
    boto3_session = boto3.session.Session()
    aws_region = boto3_session.region_name
    return aws_region

def get_ge_bucket_name(job_env):
    return f"{bdqConstants.GE_BUCKET}-{get_account_id()}-{job_env}-{get_region()}"

def get_data_bucket_name(job_env, domain_nm):
    return f"datamesh-{domain_nm}-data-domain-{get_account_id()}-{job_env}-{get_region()}"

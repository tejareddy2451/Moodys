import boto3
import datetime
from datetime import timedelta, date
from data_quality_framework.constants import bdqConstants
from data_quality_framework.write_to_bdq_tracker import update_dynamo_record

def purge_files(bucket_nm, path_to_purge, num_of_days, tracker_table, rule_id, execution_id):
    print(f'S3Purge - RULE ID:{rule_id} - Purge Files - Started')
    req_date = date.today() - timedelta(days=int(num_of_days))
    try:
        s3 = boto3.resource('s3', region_name='us-east-1')
        s3_bucket = s3.Bucket(bucket_nm)
        for file in s3_bucket.objects.filter(Prefix=path_to_purge):
            if (file.last_modified).replace(tzinfo=None) < datetime.datetime(req_date.year, req_date.month,
                                                                             req_date.day, tzinfo=None):
                print(f'S3Purge - RULE ID:{rule_id} - Purge File Name: {file.key} ---- Date: {file.last_modified}')
                file.delete()
        print(f'S3Purge - RULE ID:{rule_id} - Purge Files - Completed')
    except Exception as err:
        print(f'S3Purge - RULE ID:{rule_id} - Failed with error: {err}')
        update_dynamo_record(tracker_table, rule_id, execution_id, bdqConstants.FAILED,
                             f"S3 purge files failed with error - {err}")
        raise SystemExit(f'S3Purge - Failed with error: {err}')
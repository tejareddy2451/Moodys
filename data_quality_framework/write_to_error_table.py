import great_expectations as ge
import time
import pandas as pd
import boto3
import json
from botocore.exceptions import ClientError
from datetime import date, datetime
from data_quality_framework.bdq_common_util import get_region, get_account_id, get_data_bucket_name
from data_quality_framework.write_to_bdq_tracker import update_dynamo_record
from data_quality_framework.constants import bdqConstants, projectConstants

def create_athena_error_table(app_name, domain_nms, env):
    for domain_nm in set(domain_nms):
        print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create Athena error table - Started')
        print(
            f'WriteToErrorTable - DOMAIN:{domain_nm} - Create Athena error table - in regions {projectConstants.US_EAST_1} and {projectConstants.US_WEST_2}')
        athena_us_east_1_client = boto3.client(projectConstants.ATHENA, region_name=projectConstants.US_EAST_1)
        client_list = [athena_us_east_1_client]
        s3_file_loc = f"datamesh-{domain_nm}-data-domain-{get_account_id()}-{env}-{get_region()}/data/error/"
        bucket_name = s3_file_loc.split('/')[0]
        db_name = "datamesh_{}_error_{}".format(domain_nm, env)
        work_grp = "datamesh-{}-{}".format(app_name, env)
        table_name = bdqConstants.ATHENA_TABLE_NM
        out_loc = "s3://{}/logs/athena/{}/".format(bucket_name, table_name)
        context = {'Database': db_name}
        drop_table_query = f"DROP TABLE IF EXISTS `{table_name}`;"
        print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create Athena error table - drop_athena_raw_table - {drop_table_query}')
        create_table_query = f"""create external table `{table_name}`
                ({bdqConstants.APP_NAME} {bdqConstants.COL_TYPE_50},
                {bdqConstants.COL_EXEC_ID} {bdqConstants.COL_TYPE_50},
                {bdqConstants.DOMAIN_NAME} {bdqConstants.COL_TYPE_50},
                {bdqConstants.ZONE} {bdqConstants.COL_TYPE_50},
                {bdqConstants.RULE_ID} {bdqConstants.COL_TYPE_50},
                {bdqConstants.RULE_NAME} {bdqConstants.COL_TYPE_100},
                {bdqConstants.COL_DB_NM} {bdqConstants.COL_TYPE_50},
                {bdqConstants.DATA_TABLE_NAME} {bdqConstants.COL_TYPE_50},
                {bdqConstants.COL_PRIM_KEYS} {bdqConstants.COL_TYPE_250})
                PARTITIONED BY ({bdqConstants.RUN_DT} string)
                STORED AS PARQUET
                LOCATION 's3://{s3_file_loc}'
                """
        athena_drop_create_list = [drop_table_query, create_table_query]
        for client in client_list:
            region_name = client.meta.region_name
            print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create Athena error table -in aws region - {region_name} - Started')
            for query in athena_drop_create_list:
                print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create Athena error table - Executing the query - {query} - Started')
                res = client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext=context, WorkGroup=work_grp,
                    ResultConfiguration={'OutputLocation': out_loc})
                q_exec_id = res['QueryExecutionId']

                while True:
                    status = client.get_query_execution(QueryExecutionId=q_exec_id)
                    state = status['QueryExecution']['Status']['State']
                    if state not in ["RUNNING", "QUEUED", "CANCELED"]:
                        break
                if state == "FAILED":
                    error_message = status['QueryExecution']['Status']['StateChangeReason']
                    print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create Athena error table - Executing the query - Failed')
                    print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create Athena error table - Error - {error_message}')
                    raise Exception(f"WriteToErrorTable Athena query execution failed with Error for DOMAIN:{domain_nm} : {error_message}")
                else:
                    print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create Athena error table - Executing the query - Succeeded')
                print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create Athena error table - Executing the query - Finished')
            print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create Athena error table -in aws region - {region_name} - Finished')

            partition_qry = "MSCK REPAIR TABLE {}".format(table_name)
            print(
                f'WriteToErrorTable - DOMAIN:{domain_nm} - Create partition - Executing the query - {partition_qry} - Started')

            res = client.start_query_execution(
                QueryString=partition_qry,
                QueryExecutionContext=context,
                WorkGroup=work_grp,
                ResultConfiguration={'OutputLocation': out_loc})
            q_exec_id = res['QueryExecutionId']

            while True:
                status = client.get_query_execution(QueryExecutionId=q_exec_id)
                state = status['QueryExecution']['Status']['State']
                if state not in ["RUNNING", "QUEUED", "CANCELED"]:
                    break
            if state == "FAILED":
                error_message = status['QueryExecution']['Status']['StateChangeReason']
                print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create partition - Executing the query - Failed')
                print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create partition - Error - {error_message}')
                raise Exception(f"Athena query execution failed with Error for DOMAIN:{domain_nm} : {error_message}")
            else:
                print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create partition - Executing the query - Succeeded')
        print(f'WriteToErrorTable - DOMAIN:{domain_nm} - Create partition - Executing the query - Finished')


def send_message(app_name, job_env, tracker_table, rule_id, execution_id):
    try:
        print(f'WriteToErrorTable - RULE ID:{rule_id} - Send SNS - Started')
        message = {"BDQ Application": app_name,
                   "Dashboard URL": f"https://{bdqConstants.GE_BUCKET}-{get_account_id()}-{job_env}-{get_region()}.s3.{get_region()}.amazonaws.com/{app_name}/data_docs/index.html"}
        client = boto3.client(bdqConstants.SNS)
        response = client.publish(
            TargetArn=f"{bdqConstants.SNS_ARN_PREFIX}:{get_region()}:{get_account_id()}:{bdqConstants.FRAMEWORK}-{job_env}",
            Message=json.dumps({'default': json.dumps(message)}),
            MessageStructure=bdqConstants.JSON
        )
        print(f'WriteToErrorTable - RULE ID:{rule_id} - Send SNS - Completed')
    except ClientError as ce:
        print(f"Write to error table - send SNS - Failed with error: {ce}")
        update_dynamo_record(tracker_table, rule_id, execution_id, bdqConstants.FAILED,
                             f"send SNS failed - {ce}")
        raise(f"Write to error table - send SNS - Failed with error: {ce}")

def filter_error_records(validator, app_name, exp_type, exp_arg1, exp_arg2, key_cols, arg2_type, tracker_table, rule_id, execution_id):
    print(f'WriteToErrorTable - RULE ID:{rule_id} - Filter error records - Started')
    try:
        df = validator.head(n_rows=-1, fetch_all=True)
        ge_df = ge.from_pandas(df)
        if arg2_type == "":
            exp_suite = ge.core.ExpectationSuite(**{
                "expectation_suite_name": f"{app_name}_write",
                "expectations": [
                    {
                        "expectation_type": exp_type,
                        "kwargs": {
                            "column": exp_arg1
                        },
                        "meta": {}
                    }
                ],
            })
        elif arg2_type == "user_vals_list":
            exp_suite = ge.core.ExpectationSuite(**{
                "expectation_suite_name": f"{app_name}_write",
                "expectations": [
                    {
                        "expectation_type": exp_type,
                        "kwargs": {
                            "column": exp_arg1,
                            "value_set": exp_arg2
                        },
                        "meta": {}
                    }
                ],
            })
        elif arg2_type == "user_col_vals":
            exp_suite = ge.core.ExpectationSuite(**{
                "expectation_suite_name": f"{app_name}_write",
                "expectations": [
                    {
                        "expectation_type": exp_type,
                        "kwargs": {
                            "column": exp_arg1,
                            "min_value": int(exp_arg2.split(',')[0].strip()),
                            "max_value": int(exp_arg2.split(',')[1].strip())
                        },
                        "meta": {}
                    }
                ],
            })
        elif arg2_type == "user_col_val":
            exp_suite = ge.core.ExpectationSuite(**{
                "expectation_suite_name": f"{app_name}_write",
                "expectations": [
                    {
                        "expectation_type": exp_type,
                        "kwargs": {
                            "column_A": exp_arg1,
                            "column_B": exp_arg2
                        },
                        "meta": {}
                    }
                ],
            })
        filtered_df = ge_df.validate(exp_suite, result_format="COMPLETE")
        if "unexpected_index_list" in filtered_df.results[0].result.keys():
            unexpected_indexes = filtered_df.results[0].result["unexpected_index_list"]
            print("unexpected_indexes:",unexpected_indexes)
            failed_rows_df = df.iloc[unexpected_indexes]
            failed_rows_df = failed_rows_df.astype(str)
            print(f"Write to error table - Filter error records - sample failed records:")
            print(failed_rows_df.head())
            return True, failed_rows_df[key_cols.split(',')]
        else:
            return False, df
    except Exception as exc:
        print(f"Write to error table - Filter error records - Failed with error: {exc}")
        update_dynamo_record(tracker_table, rule_id, execution_id, bdqConstants.FAILED,
                             f"Filter error records failed - {exc}")
        raise (f"Write to error table - Filter error records - Failed with error: {exc}")

def write_error_records(app_name, domain_nm, zone, rule_id, rule_nm, db_table_nm, failed_df, job_env, tracker_table, execution_id):
    print(f'WriteToErrorTable - RULE ID:{rule_id} - Write error records - Started')
    final_df_len = int(failed_df.shape[0])
    print(f"WriteToErrorTable - RULE ID:{rule_id} - Write error records - final df len:{final_df_len}")
    try:
        if final_df_len > 0:
            final_dict = {}
            final_dict[bdqConstants.APP_NAME] = [app_name for i in range(0, final_df_len)]
            final_dict[bdqConstants.COL_EXEC_ID] = [execution_id.replace('.','') for i in range(0, final_df_len)]
            final_dict[bdqConstants.DOMAIN_NAME] = [domain_nm for i in range(0, final_df_len)]
            final_dict[bdqConstants.ZONE] = [zone for i in range(0, final_df_len)]
            final_dict[bdqConstants.RULE_ID] = [rule_id for i in range(0, final_df_len)]
            final_dict[bdqConstants.RULE_NAME] = [rule_nm for i in range(0, final_df_len)]
            final_dict[bdqConstants.COL_DB_NM] = [db_table_nm.split('.')[0] for i in range(0, final_df_len)]
            final_dict[bdqConstants.DATA_TABLE_NAME] = [db_table_nm.split('.')[1] for i in range(0, final_df_len)]
            final_dict[bdqConstants.COL_PRIM_KEYS] = [{key : val for key, val in sub.items() if key != 'index'} for sub in failed_df.reset_index().to_dict('r')]
            final_df = pd.DataFrame(final_dict)
            print("final df rec:", final_df.head())
            final_df = final_df.astype(str)
            file_loc = f"s3://{get_data_bucket_name(job_env, domain_nm)}/data/error/{bdqConstants.RUN_DT}={date.today()}/{datetime.now()}.parquet"
            print(f"WriteToErrorTable - RULE ID:{rule_id} - Write error records - file location:{file_loc}")
            final_df.to_parquet(file_loc)
        print(f'WriteToErrorTable - RULE ID:{rule_id} - Filter error records - Completed')
    except Exception as exc:
        print(f"WriteToErrorTable - RULE ID:{rule_id} - Write error records - Failed with error: {exc}")
        update_dynamo_record(tracker_table, rule_id, execution_id, bdqConstants.FAILED,
                             f"Write error records failed - {exc}")
        raise (f"Write to error table - Write error records - Failed with error: {exc}")
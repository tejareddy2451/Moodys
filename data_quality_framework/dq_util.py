import time
import ast
import pandas as pd
from itertools import groupby
from operator import itemgetter
from great_expectations.data_context import BaseDataContext
from great_expectations.core.batch import BatchRequest, RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint
from data_quality_framework.read_dynamo_input import query_bdq_inv, get_user_expectations
from data_quality_framework.postgres_data_source import postgres_datacontext
from data_quality_framework.athena_data_source import athena_datacontext
from data_quality_framework.bdq_common_util import get_tgt_db_table_name_athena, get_tgt_db_table_name_postgres, get_ge_bucket_name, get_data_bucket_name
from data_quality_framework.write_to_error_table import filter_error_records, write_error_records, create_athena_error_table
from data_quality_framework.write_to_bdq_tracker import insert_dynamo_record, update_dynamo_record
from data_quality_framework.s3_purge import purge_files
from data_quality_framework.constants import bdqConstants
from datetime import datetime
from collections import OrderedDict

def create_batch_request(datasource_nm, asset_nm, query, exp_column, key_columns, rule_id, tracker_table, execution_id):
    print(f'DQUtil - RULE ID:{rule_id} - Create Batch Request - Started')
    print(f'DQUtil - RULE ID:{rule_id} - Create Batch Request - query:{query}')
    if query != "":
        try:
            return RuntimeBatchRequest(
                datasource_name=datasource_nm,
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name=asset_nm,
                runtime_parameters={
                    "query": query},
                batch_identifiers={"default_identifier_name": "default_identifier"},
            )
        except Exception as exc:
            print(
                f"DQUtil - RULE ID:{rule_id} - Creating BatchRequest failed with error - {exc}")
            update_dynamo_record(tracker_table, rule_id, execution_id,
                                 bdqConstants.FAILED, f"Creating BatchRequest failed with error - {exc}")
            raise SystemExit(f"Creating BatchRequest failed with error - {exc}")
    else:
        if isinstance(key_columns, list) and isinstance(exp_column, list) and not all(item in key_columns for item in exp_column):
            key_columns.extend([col.lower() for col in [*set(exp_column)]])
            select_columns = ','.join(set(key_columns))
        elif isinstance(key_columns, list) and isinstance(exp_column, str) and exp_column not in key_columns:
            key_columns.append(exp_column.lower())
            select_columns = ','.join(key_columns)
        else:
            key_columns_list = key_columns.split(',')
            if exp_column not in key_columns_list:
                key_columns_list.append(exp_column)
            select_columns = ','.join(key_columns_list)
        try:
            return RuntimeBatchRequest(
                datasource_name=datasource_nm,
                data_connector_name="default_runtime_data_connector_name",
                data_asset_name=asset_nm,
                runtime_parameters={
                    "query": f"select {select_columns} from {asset_nm}"},
                batch_identifiers={"default_identifier_name": "default_identifier"},
            )
        except Exception as exc:
            print(
                f"DQUtil - RULE ID:{rule_id} - Creating BatchRequest failed with error - {exc}")
            update_dynamo_record(tracker_table, rule_id, execution_id,
                                 bdqConstants.FAILED, f"Creating BatchRequest failed with error - {exc}")
            raise SystemExit(f"Creating BatchRequest failed with error - {exc}")

def create_checkpoint_config(batch_req, app_name):
    return {
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": batch_req,
                "expectation_suite_name": f"{app_name}.{bdqConstants.EXPECTATIONS}"
            }
        ]
    }


def get_dynamo_inv_bdq_table_name(domain_name, zone, job_env, table_name):
    db_name = "datamesh" + "_" + domain_name + "_" + zone + "_" + job_env
    generated_dbtable = db_name + "." + table_name
    return generated_dbtable


def run_data_quality(job_env, app_name, run_all_rules, rule_id, purge_days):
    print(f'DQUtil - Started at f{datetime.now()}')
    execution_id = str(time.time()).replace('.', '')
    bdq_inv_table = f"{bdqConstants.BDQ_INVENTORY_TABLE}-{job_env}"
    bdq_rules_table = f"{bdqConstants.BDQ_RULES_TABLE}-{job_env}"
    bdq_tracker_table = f"{bdqConstants.BDQ_TRACKER_TABLE}-{job_env}"
    group_result = []
    print(f'DQUtil - bdq_inv_table:{bdq_inv_table}')
    print(f'DQUtil - bdq_rules_table:{bdq_rules_table}')
    print(f'DQUtil - bdq_tracker_table:{bdq_tracker_table}')
    if purge_days == '':
        purge_days = bdqConstants.PURGE_DAYS
    if run_all_rules.upper() == 'TRUE':
        dynamo_records = [inp for inp in query_bdq_inv(bdqConstants.APP_NAME, [app_name], bdq_inv_table)
                          if inp[bdqConstants.ACTIVE_FLAG] == 'Y']
        query_table_getter = itemgetter('query', 'table_name', 'zone')
        other_cols_getter = itemgetter('user_values', 'load_type', 'rule_description', 'column', 'rule_id', 'app_name', 'rule_name', 'key_columns', 'storage_type', 'domain_name')
        for (query, table_name, zone), objs in groupby(sorted(dynamo_records, key=query_table_getter),
                                          query_table_getter):
            user_values_list, load_type_list, rule_description_list, column_list, rule_id_list, app_name_list, rule_name_list, key_columns_list, storage_type_list, domain_nms_list = zip(*map(other_cols_getter, objs))
            group_result.append({
                'query': query,
                'table_name': table_name,
                'user_values': list(user_values_list),
                'zone': zone,
                'load_type': list(load_type_list),
                'rule_description': list(rule_description_list),
                'column': list(column_list),
                'rule_id': list(rule_id_list),
                'app_name': list(app_name_list),
                'rule_name': list(rule_name_list),
                'key_columns': list(key_columns_list),
                'storage_type': list(storage_type_list),
                'domain_name': list(domain_nms_list)
            })
        print("group_result:", group_result)
        dynamo_records = group_result
    else:
        dynamo_records = [inp for inp in query_bdq_inv(bdqConstants.RULE_ID, rule_id, bdq_inv_table)
                          if inp[bdqConstants.ACTIVE_FLAG] == 'Y']
    print(f'DQUtil - Total no of rules to run:{len(dynamo_records)}')
    domain_nms = []
    for dynamo_input in dynamo_records:
        exp_list = []
        insert_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id)
        if isinstance(dynamo_input[bdqConstants.DOMAIN_NAME], str):
            domain_nms.append(dynamo_input[bdqConstants.DOMAIN_NAME])
        else:
            domain_nms.extend(dynamo_input[bdqConstants.DOMAIN_NAME])
        user_exp_methods, user_exp_arg1, user_exp_args2, user_exp_arg2_type_list = get_user_expectations(dynamo_input[bdqConstants.RULE_NAME],
                                                                                                  dynamo_input[bdqConstants.COLUMN_NAME],
                                                                                                  dynamo_input[bdqConstants.USER_VALUES], bdq_rules_table,
                                                                                                  dynamo_input[bdqConstants.RULE_ID],
                                                                                                  bdq_tracker_table,
                                                                                                  execution_id)
        try:
            bucket_name = get_ge_bucket_name(job_env)
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - GE bucket name:{bucket_name}')

            db_table_name = get_dynamo_inv_bdq_table_name(dynamo_input[bdqConstants.DOMAIN_NAME][0],
                                          dynamo_input[bdqConstants.ZONE],
                                          job_env,
                                          dynamo_input[bdqConstants.DATA_TABLE_NAME])
            if dynamo_input[bdqConstants.STORAGE_TYP] == "postgres":
                db_name, schema_name, table_name = get_tgt_db_table_name_postgres(dynamo_input[bdqConstants.DATA_TABLE_NAME])
                print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Postgres DB name:{db_name}')
                print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Postgres Schema name:{schema_name}')
                print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Postgres Table name:{table_name}')
                if db_name and schema_name and table_name:
                    data_context_config = postgres_datacontext(app_name, dynamo_input[bdqConstants.SCHEMA_NAME], bucket_name, job_env)
                    print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Postgres data context creation successful')
                else:
                    print(
                        f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Postgres data context creation failed')
                    update_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id,
                                         bdqConstants.FAILED, f"Creating DataContext failed with error - Database or Schema or Table name missing for Postgres. Value should be db_name.schema_name.table_name")
                    raise ValueError("Database or Schema or Table name missing. Value should be db_name.schema_name.table_name")
            else: #dynamo_input[bdqConstants.STORAGE_TYP] == "s3":
                db_name, table_name = db_table_name.split(".")

                print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - S3 DB name:{db_name}')
                print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - S3 Table name:{table_name}')
                if db_name and table_name:
                    data_context_config = athena_datacontext(app_name, bucket_name, db_name, table_name, job_env)
                    print(
                        f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - S3 data context creation successful')
                else:
                    print(
                        f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - S3 data context creation failed')
                    update_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id,
                                         bdqConstants.FAILED,
                                         f"Creating DataContext failed with error - Database or Table name missing for S3. Value should be db_name.table_name")
                    raise ValueError("Database or Table name missing. Value should be db_name.table_name")
        except Exception as e:
            print(f"DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Creating DataContext failed with error - {e}")
            update_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id, bdqConstants.FAILED, f"Creating DataContext failed with error - {e}")
            raise SystemExit(f"Creating DataContext failed with error - {e}")

        ge_context = BaseDataContext(project_config=data_context_config)

        try:
            batch_request = create_batch_request(dynamo_input[bdqConstants.APP_NAME][0],
                                                 db_table_name,
                                                 dynamo_input[bdqConstants.QUERY], dynamo_input[bdqConstants.COLUMN_NAME],
                                                 dynamo_input[bdqConstants.KEY_COLUMNS], dynamo_input[bdqConstants.RULE_ID],
                                                 bdq_tracker_table,
                                                 execution_id)
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Create Batch Request - Completed')
        except Exception as exc:
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Create Batch Request - Failed with error - {exc}')
            update_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id, bdqConstants.FAILED,
                                 f"Creating Batch Request failed with error - {exc}")
            raise SystemExit("Creating Batch Request failed with error - ", exc)

        try:
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Create Expectation Suite - Started')
            suite = ge_context.create_expectation_suite(
                f"{dynamo_input[bdqConstants.APP_NAME][0]}.{bdqConstants.EXPECTATIONS}", overwrite_existing=True
            )
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Create Expectation Suite - Completed')
        except Exception as exc:
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Create Expectation Suite - Failed with error:{exc}')
            update_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id, bdqConstants.FAILED,
                                 f"Creating Expectation Suite failed with error - {exc}")
            raise SystemExit("Creating Expectation Suite failed with error - ", exc)
        print(f"DQUtil - RULE ID:{bdqConstants.RULE_ID} - Expectation suite name::{ge_context.list_expectation_suite_names()}")

        try:
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Create Validator - Started')
            validator = ge_context.get_validator(batch_request=batch_request, expectation_suite=suite)
            for user_exp_method in user_exp_methods:
                exp_list.append(getattr(validator,user_exp_method))
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Create Validator - Completed')
        except Exception as exc:
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Create Validator - Failed with error: {exc}')
            update_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id, bdqConstants.FAILED,
                                 f"Creating Validator failed with error - {exc}")
            raise SystemExit("Creating Validator failed with error - ", exc)

        for exp in exp_list:
            ind = exp_list.index(exp)
            user_exp_arg2 = user_exp_args2[ind]
            if user_exp_arg2 != "":
                try:
                    print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Run expectation - Started')
                    if ',' in user_exp_arg2 and isinstance(user_exp_arg2,str):
                        args = user_exp_arg2.split(',')
                        exp(user_exp_arg1[ind], int(args[0]), int(args[1]))
                    elif isinstance(user_exp_arg2, list):
                        exp(user_exp_arg1[ind], user_exp_arg2)
                    else:
                        try:
                            exp(user_exp_arg1[ind], ast.literal_eval(user_exp_arg2))
                        except ValueError:
                            exp(user_exp_arg1[ind], ast.literal_eval("'" + user_exp_arg2 + "'"))

                    print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Run expectation - Completed')
                except Exception as exc:
                    print(
                        f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Run Expectation - Failed with error: {exc}')
                    update_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id,
                                         bdqConstants.FAILED,
                                         f"Run expectation failed with error - {exc}")
                    raise SystemExit("Run expectation failed with error - ", exc)
                df_not_empty, failed_df = filter_error_records(validator, dynamo_input[bdqConstants.APP_NAME][0], user_exp_methods[ind], user_exp_arg1[ind], user_exp_arg2, dynamo_input[bdqConstants.KEY_COLUMNS][ind], user_exp_arg2_type_list[ind], bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID][ind], execution_id)

                if df_not_empty:
                    purge_files(f"{get_data_bucket_name(job_env, dynamo_input[bdqConstants.DOMAIN_NAME][0])}",
                                bdqConstants.ERROR_PATH,
                                purge_days,
                                bdq_tracker_table,
                                dynamo_input[bdqConstants.RULE_ID][ind],
                                execution_id)
                    write_error_records(dynamo_input[bdqConstants.APP_NAME][0], dynamo_input[bdqConstants.DOMAIN_NAME][ind], dynamo_input[bdqConstants.ZONE],
                                        dynamo_input[bdqConstants.RULE_ID][ind], dynamo_input[bdqConstants.RULE_NAME][ind],
                                        db_table_name,
                                        failed_df,
                                        job_env,
                                        bdq_tracker_table,
                                        execution_id)
            else:
                try:
                    print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Run expectation - Started')
                    exp(user_exp_arg1[ind])
                    print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Run expectation - Completed')
                except Exception as exc:
                    print(
                        f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Run expectation - Failed with error: {exc}')
                    update_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id,
                                         bdqConstants.FAILED,
                                         f"Run expectation failed with error - {exc}")
                    raise SystemExit("Run expectation failed with error - ", exc)
                df_not_empty, failed_df = filter_error_records(validator, dynamo_input[bdqConstants.APP_NAME][0], user_exp_methods[ind], user_exp_arg1[ind], "", dynamo_input[bdqConstants.KEY_COLUMNS][ind], user_exp_arg2_type_list[ind], bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID][ind], execution_id)

                if df_not_empty:
                    purge_files(f"{get_data_bucket_name(job_env, dynamo_input[bdqConstants.DOMAIN_NAME][ind])}",
                                bdqConstants.ERROR_PATH,
                                purge_days,
                                bdq_tracker_table,
                                dynamo_input[bdqConstants.RULE_ID][ind],
                                execution_id
                                )
                    write_error_records(dynamo_input[bdqConstants.APP_NAME][0], dynamo_input[bdqConstants.DOMAIN_NAME][ind], dynamo_input[bdqConstants.ZONE],
                                        dynamo_input[bdqConstants.RULE_ID][ind], dynamo_input[bdqConstants.RULE_NAME][ind],
                                        db_table_name,
                                        failed_df, job_env, bdq_tracker_table, execution_id)

        try:
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Save Expectations - Started')
            validator.save_expectation_suite(discard_failed_expectations=False)
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Save Expectations - Completed')
        except Exception as exc:
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Save Expectations - Failed with error:{exc}')
            update_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id,
                                 bdqConstants.FAILED,
                                 f"Saving expectations failed with error - {exc}")
            raise SystemExit("Saving expectations failed with error - ", exc)

        print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Create Checkpoint - Started')
        checkpoint_config_issuer = create_checkpoint_config(batch_request, app_name)
        checkpoint = SimpleCheckpoint(
            f"{dynamo_input[bdqConstants.APP_NAME][0]}_checkpoint",
            ge_context,
            **checkpoint_config_issuer
        )
        print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Create Checkpoint - Completed')

        try:
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Running Checkpoint - Started')
            checkpoint_result = checkpoint.run(run_name=db_table_name)
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Running Checkpoint - Completed')
        except Exception as exc:
            print(
                f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Running Checkpoint - Failed with error:{exc}')
            update_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id,
                                 bdqConstants.FAILED,
                                 f"Running Checkpoint failed with error - {exc}")
            raise SystemExit("Running Checkpoint failed with error - ", exc)

        try:
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Build Data Docs - Started')
            ge_context.build_data_docs()
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Build Data Docs - Completed')
        except Exception as exc:
            print(f'DQUtil - RULE ID:{dynamo_input[bdqConstants.RULE_ID]} - Build Data Docs - Failed with error: {exc}')

        update_dynamo_record(bdq_tracker_table, dynamo_input[bdqConstants.RULE_ID], execution_id, bdqConstants.SUCCESS,"")
    create_athena_error_table(app_name, domain_nms, job_env)
    return
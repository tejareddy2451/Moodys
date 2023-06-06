# System Libraries
import sys
import concurrent.futures
# User Libraries
from ingestion_framework.connector_framework.connector_utils import InventoryReader
from ingestion_framework.scripts import job_initializer
from ingestion_framework.scripts import PipelineRunner
from ingestion_framework.constants import projectConstants
from ingestion_framework.utils import logger_util
from ingestion_framework.constants import dynamodbConstants
from ingestion_framework.utils import dynamodb_util
from datetime import date, timedelta, datetime


#To trigger all pipelines in parallel
def trigger_pipelines_in_parallel(pipelines_with_versions, app_name, job_env, job_start_datetime, job_end_datetime, spark, logger):
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for pipeline_id, version in pipelines_with_versions.items():
            futures.append(
                executor.submit(
                PipelineRunner.run, app_name, pipeline_id, version, job_env, job_start_datetime, job_end_datetime, spark, logger
                        )
            )
    
    #done, not_done = wait(futures, return_when=ALL_COMPLETED)
    #as_completed(futures, timeout=None)
    print('end of concurrent futures execution')
    future_result = []
    for future in concurrent.futures.as_completed(futures):
        if future.result() is not None :
            future_result.append(future.result())
     
    executor.shutdown()
    print('concurrent futures shutdown....')
    if len(future_result) > 0:
        print(f"Failed pipelines are : {future_result}")
        raise SystemExit(f'Some Pipelines failed to load the data : {future_result}')


#To trigger all pipelines in sequence
def trigger_pipelines_in_sequence(pipelines_with_versions, app_name, job_env, job_start_datetime, job_end_datetime, spark, logger):
    for pipeline_id, version in pipelines_with_versions.items():
        PipelineRunner.run(app_name, pipeline_id, version, job_env, job_start_datetime, job_end_datetime, spark, logger)

def get_delta_days(job_start_datetime, job_end_datetime):
    start_dt = date(int(job_start_datetime.split('-')[0]), int(job_start_datetime.split('-')[1]), int(job_start_datetime.split('-')[2]))
    end_dt = date(int(job_end_datetime.split('-')[0]), int(job_end_datetime.split('-')[1]), int(job_end_datetime.split('-')[2]))
    delta = end_dt - start_dt
    days = [start_dt + timedelta(days=i) for i in range(delta.days + 1)]
    days = [str(dt.strftime("%Y-%m-%d")) for dt in days]
    print(f"App Runner - total no of days to process = {len(days)}")
    return days

def get_prev_execution_status(spark, logger, job_env, pipeline_id):
    dynamo_table_name = dynamodbConstants.JOB_STATUS_TABLE.format(job_env)
    pipeline_metadata = dynamodb_util.JobStatus(spark, logger).query_jobstatus(dynamo_table_name, pipeline_id)
    exec_time_status_dict = {}
    for rec in pipeline_metadata:
        exec_time_status_dict[rec['execution_start_dttm']] = rec['job_status']
    sorted_exec_times = list(reversed(sorted(exec_time_status_dict.keys())))
    for last_exec_time in sorted_exec_times:
        last_exec_status = exec_time_status_dict[last_exec_time]
        if last_exec_status == 'SUCCESS':
            return last_exec_time
        else:
            continue
    return str(datetime(1900, 1, 1, 00, 00, 00, 00))

def run(app_name, run_all_flag, pipeline_id, version, job_env, job_start_datetime, job_end_datetime, run_in_chunks_flag,sequence_run):
    #1. App name is Mandatory
    if not app_name:
        # logger.error('AppRunner - Application Name is needed')
        raise SystemExit('Application Name is needed')
    
    # Initializing spark and Logger
    spark = job_initializer.initializeSpark(app_name)   
    logger = job_initializer.getGlueLogger(spark)
    
    print('AppRunner - Started')

    #Printing all given parameters
    print('AppRunner - Read all the job parameters.')
    print(f'AppRunner - App Name : {app_name}')
    print(f'AppRunner - Run All Parameters flag : {run_all_flag}')
    print(f'AppRunner - Pipeline Id : {pipeline_id}')
    print(f'AppRunner - Version : {version}')
    print(f'AppRunner - Job Environment : {job_env}')
    print(f'AppRunner - Job Start Date time : {job_start_datetime}')
    print(f'AppRunner - Job End Date time : {job_end_datetime}')
    print(f'AppRunner - Run In Chunks Flag : {run_in_chunks_flag}')

    #Validations
    
    #1. Job Environment is Mandatory
    if not job_env:
        logger.error('AppRunner - Job Environment is needed')
        print('AppRunner - Job Environment is needed')
        raise SystemExit('Job Environment is needed')
    
    #2. When Run all parameter is false, pipeline_id is needed
    if run_all_flag != "true" and not pipeline_id:
        logger.error('AppRunner - Pipeline id is needed when Run All Pipelines is false')
        print('AppRunner - Pipeline id is needed when Run All Pipelines is false')
        raise SystemExit('AppRunner - Pipeline id is needed when Run All Pipelines is false')
    
    #3. Version validation
    if not version:
        print('AppRunner - Version is not given. So proceeding with the latest active version.')
        
    
    required_pipelines_with_versions = {}
    error_pipelines = []
    
    #Run all enabled block
    if run_all_flag == "true":
        print('AppRunner - Run all pipelines flag is enabled')
        
        #Get all the pipeline details with latest active versions
        required_pipelines_with_versions = InventoryReader.get_all_pipelineids_with_latest_versions(app_name)     
    
    #Run all disabled block
    else:
        print('AppRunner - Run all pipelines flag is not enabled')
        #Processing multiple pipeline ids in pipeline_id field
        if "," in pipeline_id:
            print('AppRunner - Multiple pipeline ids were given in pipeline_id')
            pipelines_with_versions = InventoryReader.get_all_pipelineids_with_latest_versions(app_name)
            
            pipeline_ids = pipeline_id.split(",")
            
            for p_id in pipeline_ids:
                p_id = p_id.strip()
                if p_id in pipelines_with_versions:
                    required_pipelines_with_versions[p_id] = pipelines_with_versions[p_id]
                else:
                    error_pipelines.append(p_id)
        #Processing single pipeline id with latest version 
        elif not version:
            print('AppRunner - Version is not given for the pipeline_id. So fetching the latest active version number.')
            pipelines_with_versions = InventoryReader.get_all_pipelineids_with_latest_versions(app_name)
            if pipeline_id in pipelines_with_versions:
                required_pipelines_with_versions[pipeline_id] = pipelines_with_versions[pipeline_id]
            else:
                error_pipelines.append(pipeline_id) 
        #Processing single pipeline id with given version
        else:
            print('AppRunner - Version is given for pipeline_id.')
            pipelines_with_versions = InventoryReader.get_all_pipelineids_with_latest_versions(app_name)
            if pipeline_id in pipelines_with_versions and pipelines_with_versions[pipeline_id] == version:
                required_pipelines_with_versions[pipeline_id] = version
            else:
                error_pipelines.append(pipeline_id)
     
    #Printing the list of pipelines and versions identified
    if len(required_pipelines_with_versions) == 0:
        print('AppRunner - No pipelines found with valid pipeline_id, version and active status.')
        raise SystemExit('AppRunner - No pipelines found with valid pipeline_id, version and active status.')
    else:
        print('AppRunner - List of pipelines with versions identified:')
        for p_id,v in required_pipelines_with_versions.items():
            print(f'AppRunner - Pipeline Id:{p_id}  -> Version:{v}')
            prev_exec_time = get_prev_execution_status(spark, logger, job_env, p_id)
        #Triggering all pipelines in parallel
        print('AppRunner - Triggering all pipelines in parallel')
        if run_in_chunks_flag.upper()=='TRUE':
            if job_start_datetime != "":
                start_date = job_start_datetime.split(' ')[0]
                end_date = job_end_datetime.split(' ')[0] if job_end_datetime != "" else str(datetime.now()).split(' ')[0]
            else:
                start_date = prev_exec_time.split(' ')[0]
                end_date = str(datetime.now()).split(' ')[0]
            delta_days = get_delta_days(start_date, end_date)
            # job_start_datetime = prev_exec_time
            for day in delta_days:
                print(f'AppRunner - day is {day}')
                # Get End DateTime
                if str(day) != str(end_date):
                    end_time = str(day)+" 23:59:59"
                else:
                    end_time = job_end_datetime if job_end_datetime != "" else str(datetime.now())
                if str(day) == str(start_date) and job_start_datetime != "":
                    print(f'AppRunner - triggering pipeline for the window {job_start_datetime} and {end_time}')
                    trigger_pipelines_in_parallel(required_pipelines_with_versions, app_name, job_env,
                                                  job_start_datetime, end_time, spark, logger)
                elif str(day) == str(start_date) and job_start_datetime == "":
                    print(f'AppRunner - triggering pipeline for the window {prev_exec_time} and {end_time}')
                    trigger_pipelines_in_parallel(required_pipelines_with_versions, app_name, job_env,
                                                  prev_exec_time, end_time, spark, logger)
                else:
                    print(f'AppRunner - triggering pipeline for the window {str(day)+" 00:00:00"} and {end_time}')
                    trigger_pipelines_in_parallel(required_pipelines_with_versions, app_name, job_env,
                                                  str(day)+" 00:00:00", end_time, spark, logger)
        else:
            if  sequence_run == "true":
                trigger_pipelines_in_sequence(required_pipelines_with_versions, app_name, job_env, job_start_datetime, job_end_datetime, spark, logger)
            else:
                trigger_pipelines_in_parallel(required_pipelines_with_versions, app_name, job_env, job_start_datetime, job_end_datetime, spark, logger)
    if len(error_pipelines) > 0:
        print(f'AppRunner - The following pipeline ids were either not valid or not active : {error_pipelines}')
        raise SystemExit(f'AppRunner - The following pipeline ids were either not valid or not active : {error_pipelines}')

    print('AppRunner - Completed')
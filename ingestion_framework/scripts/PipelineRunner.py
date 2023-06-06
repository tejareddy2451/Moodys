# System Libraries
import sys
import time
from datetime import datetime, timedelta
from pytz import timezone
# User Libraries
from ingestion_framework.connector_framework.connector_utils import InventoryReader
from ingestion_framework.connector_framework.connector_utils import ConnectorSupplier
from ingestion_framework.utils import s3_util
from ingestion_framework.utils import common_util
from ingestion_framework.utils import column_util
from ingestion_framework.utils import scd_util
from ingestion_framework.utils import dynamodb_util
from ingestion_framework.utils import logger_util
from ingestion_framework.scripts import job_initializer
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants


#@logger_util.common_logger_exception
def run(app_name, pipeline_id, version, job_env, job_start_datetime, job_end_datetime, spark, logger):
    print(f'PipelineRunner - pipeline id:{pipeline_id} - Started')
    print(f'PipelineRunner - pipeline id:{pipeline_id} - App Name:{app_name}')
    print(f'PipelineRunner - pipeline id:{pipeline_id} - Pipeline Id:{pipeline_id}')
    print(f'PipelineRunner - pipeline id:{pipeline_id} - Version:{version}')
    print(f'PipelineRunner - pipeline id:{pipeline_id} - Job Environment:{job_env}')
    print(f'PipelineRunner - pipeline id:{pipeline_id} - Job Start Date time:{job_start_datetime}')
    print(f'PipelineRunner - pipeline id:{pipeline_id} - Job End Date time:{job_end_datetime}')
    job_start_time = datetime.now(timezone('US/Eastern'))
    curr_time = job_start_time.time()
    exec_id = projectConstants.NONE
    sla_met = 'false'
    inv_dict = {}
    business_order_from_dt = projectConstants.NONE
    business_order_to_dt = projectConstants.NONE
    job_status = dynamodb_util.JobStatus(spark, logger)
    
    try:

        print(f'PipelineRunner - pipeline id:{pipeline_id} - Fetching the details from yaml file - Started')
        inv_dict = InventoryReader.get_pipeline_dict_from_yaml(app_name, pipeline_id, version)
        print(f'PipelineRunner - pipeline id:{pipeline_id} - Fetching the details from yaml file - Finished')

        print(f'PipelineRunner - pipeline id:{pipeline_id} - Setting initial job status - Started')
        exec_id = job_status.put_jobstatus(inv_dict, str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), job_env)
        print(f'PipelineRunner - pipeline id:{pipeline_id} - Setting initial job status - Finished')

        print(f'PipelineRunner - pipeline id:{pipeline_id} - Checking ola time range - Started')
        ola = inv_dict[dynamodbConstants.OLA]
        if ola is None:
            ola = ''
        ola_list = ola.split('-') if '-' in ola else ['', '']
        ola_start_time = ola_list[0].strip()
        ola_end_time = ola_list[1].strip()
        ola_start_time = datetime.strptime(ola_start_time, '%H:%M:%S').time() if ola_start_time != '' else ola_start_time
        ola_end_time = datetime.strptime(ola_end_time, '%H:%M:%S').time() if ola_end_time != '' else ola_end_time
        if ola_start_time == '' or ola_end_time == '' or (not ola_start_time <= curr_time <= ola_end_time):
            print(f'PipelineRunner - pipeline id:{pipeline_id} - Checking ola time range - Finished')

            if inv_dict[dynamodbConstants.SOURCE_TYPE] != projectConstants.S3:
                #Setting this time to make sure delta queries take current job's start time as end time for query formation
                job_end_datetime = str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")) if job_end_datetime == '' else job_end_datetime
                updated_query, business_order_from_dt, business_order_to_dt = common_util.get_source_query(spark, logger, inv_dict, pipeline_id, job_start_datetime, job_end_datetime,job_env)
                #print(f'PipelineRunner - pipeline id:{pipeline_id} - Updated Query - {updated_query}')
                inv_dict[dynamodbConstants.SOURCE_QUERY] = updated_query

            print(f'PipelineRunner - pipeline id:{pipeline_id} - Creating source configuration')
            src_dict = InventoryReader(inv_dict).get_source_connector_config(job_start_datetime, job_end_datetime)
            print(f'PipelineRunner - pipeline id:{pipeline_id} - Creating target configuration')
            tgt_dict = InventoryReader(inv_dict).get_target_connector_config(job_start_datetime, job_end_datetime)
            
            print(f'PipelineRunner - pipeline id:{pipeline_id} - Creating source connector')
            src_connector = ConnectorSupplier(spark, logger, job_env).get_connector(src_dict)
            print(f'PipelineRunner - pipeline id:{pipeline_id} - Creating target connector')
            tgt_connector = ConnectorSupplier(spark, logger, job_env).get_connector(tgt_dict)

            print(f'PipelineRunner - pipeline id:{pipeline_id} - Read - Started')
            landing_df = src_connector.read_data().cache()
            landing_count = landing_df.count()
            print(f'PipelineRunner - pipeline id:{pipeline_id} - Read - Finished')

            if landing_count>0:
                if tgt_dict.connector_name == projectConstants.S3:
                    print(f'PipelineRunner - pipeline id:{pipeline_id} - Write - Landing - Started')
                    tgt_connector.write_data(landing_df, write_location = projectConstants.DEFAULT_S3_WRITE_LANDING_PREFIX)
                    print(f'PipelineRunner - pipeline id:{pipeline_id} - Write - Landing - Finished')
                print(f'PipelineRunner - pipeline id:{pipeline_id} - Sorting - Casting - landing_df started')
                landing_df = column_util.ColumnParser(app_name, spark, landing_df, inv_dict, logger).column_parser_runner()
                print(f'PipelineRunner - pipeline id:{pipeline_id} - Sorting - Casting - landing_df finished')
                
                
                print(f'PipelineRunner - pipeline id:{pipeline_id} - scd process - landing_df started')
                if inv_dict[dynamodbConstants.LOAD_TYPE]==projectConstants.DELTA_LOAD:
                    full_tgt_df=tgt_connector.read_tgt_data()
                    final_df = scd_util.SCD(spark, logger, inv_dict, tgt_dict, app_name, job_env, landing_df,full_tgt_df).scd_process()
                    print(f'PipelineRunner - pipeline id:{pipeline_id} - scd process - landing_df finished')
                else:
                    print(f'PipelineRunner - pipeline id:{pipeline_id} - The pipeline is a full load')
                    final_df=landing_df
                final_count = final_df.count()


                print(f'PipelineRunner - pipeline id:{pipeline_id} - Write - Final - Started')
                tgt_connector.write_data(final_df, write_location = projectConstants.DEFAULT_S3_WRITE_FINAL_PREFIX)
                print(f'PipelineRunner - pipeline id:{pipeline_id} - Write - Final - Finished')
            else:
                print(f'PipelineRunner - pipeline id:{pipeline_id} -There is no new records from the source')
                final_count=0
                
            # Determine if sla for job runtime is met successfully
            print(f'PipelineRunner - pipeline id:{pipeline_id} - SLA verificatin - Started')
            job_end_time = datetime.now(timezone('US/Eastern'))
            sla_met = common_util.get_sla_met(job_start_time, job_end_time, inv_dict, logger)
            print(f'PipelineRunner - pipeline id:{pipeline_id} - SLA verificatin - Finished')

            print(f'PipelineRunner - pipeline id:{pipeline_id} - Updating job status - Started')
            job_status.update_jobstatus(inv_dict, job_env, pipeline_id, exec_id, str(business_order_from_dt), str(business_order_to_dt), str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")) , str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")), projectConstants.SUCCESS_FLAG, '', str(landing_count), str(final_count), 0, sla_met)
            print(f'PipelineRunner - pipeline id:{pipeline_id} - Updating job status - Finished')
            print(f'PipelineRunner - pipeline id:{pipeline_id} - Completed')
        else:
            raise Exception('Aborted due to job start time between OLA time range')
    except Exception as err:
        print(f'PipelineRunner - pipeline id:{pipeline_id} - Failed with error: {err}')
        print(f'PipelineRunner - pipeline id:{pipeline_id} - Updating job status - Started')
        job_end_time = datetime.now(timezone('US/Eastern'))
        job_status.update_jobstatus(inv_dict, job_env, pipeline_id, exec_id, str(business_order_from_dt), str(business_order_to_dt), str(job_start_time.strftime("%Y-%m-%d %H:%M:%S")), str(job_end_time.strftime("%Y-%m-%d %H:%M:%S")), projectConstants.FAILURE_FLAG, str(err), 0, 0, 0, sla_met)
        print(f'PipelineRunner - pipeline id:{pipeline_id} - Updating job status - Finished')
        print(f'PipelineRunner - pipeline id:{pipeline_id} - Completed')
        return {pipeline_id}


        
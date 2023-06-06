import sys
import ast
from datetime import datetime, timedelta

from pyspark.sql.functions import lit
from ingestion_framework.constants import *
from ingestion_framework.utils import common_util, s3_util
from ingestion_framework.utils import logger_util


import json

class ConfigParser(object):
    #@logger_util.common_logger_exception
    def __init__(self, config_dict):
        self.__dict__.update(config_dict)

#@logger_util.common_logger_exception
def dict2obj(configDict):
    return json.loads(json.dumps(configDict), object_hook=ConfigParser)


class SCD():
    #@logger_util.common_logger_exception
    def __init__(self, spark, logger,inv_dict , tgtConfig, app_name, job_env,incremental_df=None,full_tgt_df=None):
        print(f'SCD Util - {inv_dict[dynamodbConstants.PIPELINE_ID]} - hit SCD')
        self.spark = spark
        self.logger = logger
        self.inv_dict = inv_dict
        self.yaml = dict2obj(self.inv_dict) 
        self.tgtConfigFull = tgtConfig
        self.tgtConfig = tgtConfig.resource_details
        self.app_name = app_name
        self.job_env = job_env
        self.incremental_df=incremental_df
        self.full_tgt_df=full_tgt_df
        
        self.p_key = self.inv_dict[dynamodbConstants.PRIMARY_KEY_SET]
        self.cdc_key = self.inv_dict[dynamodbConstants.CDC_TIMESTAMP_KEY]
        self.log_chng_key = self.inv_dict[dynamodbConstants.LOG_CHNG_INDICATOR_KEY]

        if self.p_key is not None and self.p_key != '' and self.p_key != 'null' and self.p_key != []:
            self.p_key = ast.literal_eval(self.inv_dict[dynamodbConstants.PRIMARY_KEY_SET])
        else:
            self.p_key = []

        if self.cdc_key is not None and self.cdc_key != '' and self.cdc_key != 'null' and self.cdc_key != []:
            self.cdc_key = ast.literal_eval(self.inv_dict[dynamodbConstants.CDC_TIMESTAMP_KEY])
        else:
            self.cdc_key = []

        if self.log_chng_key is not None and self.log_chng_key != '' and self.log_chng_key != 'null' and self.log_chng_key != []:
            self.log_chng_key = ast.literal_eval(self.inv_dict[dynamodbConstants.LOG_CHNG_INDICATOR_KEY])
        else:
            self.log_chng_key = []

    #@logger_util.common_logger_exception
    def scd_process(self):
            rdf = self.delta_load()
            return rdf

    #@logger_util.common_logger_exception
    def delta_load(self):
        print(f'SCD Util - {self.yaml.pipeline_id} - hit Delta Load')
        # Receive Yaml variables values
        target_details = common_util.get_target_environment_details(self.tgtConfig, self.job_env)
        target_bucket_name = target_details[dynamodbConstants.TARGET_BUCKET_NAME]
        file_name = self.tgtConfig[dynamodbConstants.TARGET_FILE_NAME]
        incremental_df=self.incremental_df
        if incremental_df:
            print(f'SCD Util - {self.yaml.pipeline_id} - Incremental DF count : {incremental_df.count()}')
        print(f'SCD Util - {self.yaml.pipeline_id} - Incremental DF - Ready')

        print(f'SCD Util - {self.yaml.pipeline_id} - creating target dataframe started ')
        full_data_file_df= self.full_tgt_df
        print(f'SCD Util - {self.yaml.pipeline_id} - creating target dataframe finished')
        if full_data_file_df:
            print(f'SCD Util - {self.yaml.pipeline_id} - full_data_file_df DF count : {full_data_file_df.count()}')
        #Create Incremental DF View
        incremental_vw_name = f'{file_name}_incremental_vw'
        full_data_file_vw_name = f'{file_name}_full_data_file_vw'
        incremental_df.createOrReplaceTempView(incremental_vw_name)
        print(f"SCD Util - {self.yaml.pipeline_id} incremental temp view created")
        print(f"SCD Util - {self.yaml.pipeline_id} target column----{self.inv_dict[dynamodbConstants.TARGET_PARTITION_COL]}")
        #FullDataFile - Has Some Data
        if full_data_file_df:
                print(f'SCD Util - {self.yaml.pipeline_id} - Full Data DF - Ready')

                #Create Full Data DF View
                full_data_file_df.createOrReplaceTempView(full_data_file_vw_name)

                #Apply CDC
                join_cond = self.get_join_condition(self.p_key)
                print(f'SCD Util - {self.yaml.pipeline_id} - Join Condition - {join_cond}')

                #LogChangeIndicator - Given. Assumption, we will get a single column here           
                if len(self.log_chng_key) >= 1:
                    print(f'SCD Util - {self.yaml.pipeline_id} - LogIndicatorChangeKey - Given - {self.log_chng_key}')
                    log_chang_key_column = self.log_chng_key[0]

                    supported_updates = ''
                    for supported_update in projectConstants.CDC_SUPPORTED_UPDATES:
                        supported_updates += f"\"{supported_update}\","
                    if supported_updates != '':
                        supported_updates = "("+supported_updates[:-1]+")"
                    
                    upd_df = self.spark.sql(("select incr.* From {} incr left join {} full_file on {} where incr.{} in {}")
                                            .format(incremental_vw_name,
                                            full_data_file_vw_name, join_cond, log_chang_key_column, 
                                            supported_updates))

                    print(f'SCD Util - {self.yaml.pipeline_id} - Update DF - Ready')

                    supported_inserts = ''
                    for supported_insert in projectConstants.CDC_SUPPORTED_INSERTS:
                        supported_inserts += f"\"{supported_insert}\","
                    if supported_inserts != '':
                        supported_inserts = "("+supported_inserts[:-1]+")"

                    ins_df = self.spark.sql(("select incr.* from {} incr where incr.{} in {} ")
                                            .format(incremental_vw_name,
                                            log_chang_key_column, supported_inserts))

                    print(f'SCD Util - {self.yaml.pipeline_id} - Insert DF - Ready')

                    print(f"SCD Util - {self.yaml.pipeline_id} - Combining Inserts and Updates")
                    ins_upd_df = upd_df.unionByName(ins_df)
                    print(f'SCD Util - {self.yaml.pipeline_id} - Insert and Updates Count: ', ins_upd_df.count())

                    #Drop unnecessary columns
                    ins_upd_df = ins_upd_df.drop(log_chang_key_column)
                    if 'rno' in ins_upd_df.columns:
                        ins_upd_df = ins_upd_df.drop('rno')
                    
                    print(f'SCD Util - {self.yaml.pipeline_id} - Dropping unnecessary columns - Done')
                    #Add etl_insert_ts column
                    #ins_upd_df = ins_upd_df.withColumn(projectConstants.ETL_INSERT_TS, lit(common_util.get_current_timestamp(1)))

                    #print(f'SCD Util - {self.yaml.pipeline_id} - Adding etl_insert_ts column - Done')

                    unchanged_df = self.spark.sql((" select full_file.* \
                                                   From {} full_file\
                                                   Left join {} incr on {}\
                                                   where incr.{} is null ").format(full_data_file_vw_name,
                                                        incremental_vw_name, join_cond, log_chang_key_column))

                    print(f'SCD Util - {self.yaml.pipeline_id} - Unchanged rows count: ', unchanged_df.count())
                    print(f"SCD Util - {self.yaml.pipeline_id} - Combining Inserts and Updates with Unchanged rows")
                    rdf = unchanged_df.unionByName(ins_upd_df)

                    #Handling deletes
                    #delete_df = self.spark.sql(("select incr.*  \
                    #                      from {} incr \
                    #                       where incr.{} in {} ").format(incremental_vw_name, log_chang_key_column, 
                    #                            projectConstants.CDC_SUPPORTED_DELETES))

                    print(f'SCD Util - {self.yaml.pipeline_id} - Final rows count', rdf.count())
                    return rdf
                
                #LogChangeIndicator - Not Given, Target Partition Column - Not Given
                elif self.inv_dict[dynamodbConstants.TARGET_PARTITION_COL] is None or self.inv_dict[dynamodbConstants.TARGET_PARTITION_COL] == '':
                    print(f'SCD Util - {self.yaml.pipeline_id} - LogIndicatorChangeKey - Not Given')
                    print(f'SCD Util - {self.yaml.pipeline_id} - Target Partition Column - Not Given')
                    #Primary Key - Given
                    if self.p_key is not None and self.p_key != []:
                        print(f'SCD Util - {self.yaml.pipeline_id} - Primary Key - Given')
                        upd_df = self.spark.sql(("select incr.* \
                                        From {} incr \
                                        join {} full_file on {}").format(incremental_vw_name,
                                            full_data_file_vw_name, join_cond))
                        
                        print(f'SCD Util - {self.yaml.pipeline_id} - Update DF - Ready')
                        print(f'SCD Util - {self.yaml.pipeline_id} - Updates Count: ', upd_df.count())
                    
                        #ins_upd_df = ins_upd_df.withColumn(projectConstants.ETL_INSERT_TS, lit(common_util.get_current_timestamp(1)))

                        #print(f'SCD Util - {self.yaml.pipeline_id} - Adding etl_insert_ts column - Done')

                        unchanged_df = full_data_file_df.subtract(upd_df)

                        print(f'SCD Util - {self.yaml.pipeline_id} - Unchanged rows count: ', unchanged_df.count())

                        print(f"SCD Util - {self.yaml.pipeline_id} - Combining Inserts and Updates with Unchanged rows")
                        rdf = unchanged_df.unionByName(incremental_df)

                        print(f'SCD Util - {self.yaml.pipeline_id} - Final rows count', rdf.count())
                        return rdf
                    #Primary Key - Not Given
                    else:
                        print(f"SCD Util - {self.yaml.pipeline_id} - Primary Key - Not Given")
                        rdf = incremental_df.unionByName(full_data_file_df)
                        #rdf = incremental_df.withColumn(projectConstants.ETL_INSERT_TS, lit(common_util.get_current_timestamp(1)))

                        print(f'SCD Util - {self.yaml.pipeline_id} - Final rows count', rdf.count())
                        return rdf
                 
                #LogChangeIndicator - Not Given, Target Partition Column - Given
                elif self.inv_dict[dynamodbConstants.TARGET_PARTITION_COL] is not None:
                    print(f'SCD Util - {self.yaml.pipeline_id} - LogIndicatorChangeKey - Not Given')
                    print(f'SCD Util - {self.yaml.pipeline_id} - Target Partition Column - Given')

                    rdf = incremental_df
                    #rdf = incremental_df.withColumn(projectConstants.ETL_INSERT_TS, lit(common_util.get_current_timestamp(1)))
                    #print(f'SCD Util - {self.yaml.pipeline_id} - Adding etl_insert_ts column - Done')

                    print(f'SCD Util - {self.yaml.pipeline_id} - Final rows count', rdf.count())
                    return rdf
        
        print(f'SCD Util - {self.yaml.pipeline_id} -target table does not exist')
        
        rdf = incremental_df
        #rdf = incremental_df.withColumn(projectConstants.ETL_INSERT_TS, lit(common_util.get_current_timestamp(1)))
        #print(f'SCD Util - {self.yaml.pipeline_id} - Adding etl_insert_ts column - Done')

        print(f'SCD Util - {self.yaml.pipeline_id} - Final rows count', rdf.count())
        return rdf
                    

    #@logger_util.common_logger_exception
    def cdc_type4(self):
        print('SCD Util - hit cdc_type4')
        fnl_df = self.spark.read.format(projectConstants.PARQUET).load(self.path_fnl)
        if fnl_df is None:
            rdf = self.df.withColumn(projectConstants.ETL_INSERT_TS, lit(common_util.get_current_timestamp(1)))
            return rdf
        deltaTable = fnl_df.join(self.df, on=self.p_key, how='anti').union(self.df)
        rdf = deltaTable.withColumn(projectConstants.ETL_INSERT_TS, lit(common_util.get_current_timestamp(1)))
        return rdf

    #@logger_util.common_logger_exception
    def get_join_condition(self, key_columns):
        print('SCD Util - converted key_columns {} to list'.format(key_columns))
        join_cond = ''
        index = 0
        if len(key_columns) >= 1:
            for col in key_columns:
                join_cond = join_cond + ' full_file.' + col + ' = ' + 'incr.' + col + ' and'
                partition_columns = ' incr.' + col + ','
            index = join_cond.rfind("and")
            comma_index = partition_columns.rfind(",")
            join_cond = join_cond[:index]
            partition_columns = partition_columns[:comma_index]
        return join_cond

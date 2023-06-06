import ast
from ingestion_framework.utils import logger_util
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants


class ColumnParser:
    #@logger_util.common_logger_exception
    def __init__(self, app_name, spark, read_df, inv_dict, logger):
        self.spark = spark
        self.app_name = app_name
        self.read_df = read_df
        self.inv_dict = inv_dict
        self.pipeline_id = inv_dict[dynamodbConstants.PIPELINE_ID]
        self.logger = logger

    #@logger_util.common_logger_exception
    def sort_columns_dicts_lst(self):
        """Sorts columns based on sequence number"""
        pipeline_columns = ast.literal_eval(self.inv_dict[dynamodbConstants.PIPELINE_COLUMNS])
        sorted_pipeline_columns_list = sorted(pipeline_columns, key=lambda d: int(d[dynamodbConstants.SEQUENCE_NUMBER]))
        print(f"Column Util - {self.pipeline_id} - Sorted Columns: {sorted_pipeline_columns_list}")
        return sorted_pipeline_columns_list


    #@logger_util.common_logger_exception
    def column_parser_runner(self):
        """Runs all the functions in the column parser"""
        print(f"Column Util - {self.pipeline_id} - creating a temp view on csv read df")
        self.temp_view_name = self.pipeline_id.replace("-","_")
        self.read_df.createOrReplaceTempView(self.temp_view_name)

        # Building the dynamic sql and running it on temp view
        fl_df = self.spark.sql(self.cast_src_to_tgt_types())
        return fl_df

    #@logger_util.common_logger_exception
    def cast_src_to_tgt_types(self):
        """Function casts source types to target types for all columns"""
        sorted_cols_dicts_lst = self.sort_columns_dicts_lst()
        query_string = ''
        sep = ','

        for sorted_cols_dict in sorted_cols_dicts_lst:
            if sorted_cols_dict[dynamodbConstants.TARGET_DATATYPE].lower() == "timestamp":
                query_string += str(f'cast(date_format({sorted_cols_dict[dynamodbConstants.SOURCE_COLUMN_NAME]},\'{sorted_cols_dict[dynamodbConstants.TARGET_DATETIME_FORMAT]}\') as {sorted_cols_dict[dynamodbConstants.TARGET_DATATYPE]})')
            elif sorted_cols_dict[dynamodbConstants.TARGET_DATATYPE].lower() == "date":
                query_string += str(f'cast(to_date({sorted_cols_dict[dynamodbConstants.SOURCE_COLUMN_NAME]},\'{sorted_cols_dict[dynamodbConstants.TARGET_DATETIME_FORMAT]}\') as {sorted_cols_dict[dynamodbConstants.TARGET_DATATYPE]})')
            elif sorted_cols_dict[dynamodbConstants.TARGET_DATATYPE].lower() == "char" or sorted_cols_dict[dynamodbConstants.TARGET_DATATYPE].lower() == "varchar":
                    query_string += str(f"cast({sorted_cols_dict[dynamodbConstants.SOURCE_COLUMN_NAME]} as string)")
            elif dynamodbConstants.TARGET_LENGTH in sorted_cols_dict and sorted_cols_dict.get(dynamodbConstants.TARGET_LENGTH) !=None and sorted_cols_dict.get(dynamodbConstants.TARGET_LENGTH) !="":
                if dynamodbConstants.TARGET_PRECISION in sorted_cols_dict and sorted_cols_dict.get(dynamodbConstants.TARGET_PRECISION) !=None and sorted_cols_dict.get(dynamodbConstants.TARGET_PRECISION) !="":
                    query_string += str(f"cast({sorted_cols_dict[dynamodbConstants.SOURCE_COLUMN_NAME]} as {sorted_cols_dict[dynamodbConstants.TARGET_DATATYPE]}({sorted_cols_dict.get(dynamodbConstants.TARGET_LENGTH)}, {sorted_cols_dict.get(dynamodbConstants.TARGET_PRECISION)}))")
                else:
                    query_string += str(f"cast({sorted_cols_dict[dynamodbConstants.SOURCE_COLUMN_NAME]} as {sorted_cols_dict[dynamodbConstants.TARGET_DATATYPE]})")
            else:
                query_string += str(f"cast({sorted_cols_dict[dynamodbConstants.SOURCE_COLUMN_NAME]} as {sorted_cols_dict[dynamodbConstants.TARGET_DATATYPE]})")
                                        
            query_string += str(f" as {sorted_cols_dict[dynamodbConstants.TARGET_COLUMN_NAME]}" + sep)

        log_chng_key = self.inv_dict[dynamodbConstants.LOG_CHNG_INDICATOR_KEY]
        if log_chng_key is not None and log_chng_key != '' and log_chng_key != 'null' and log_chng_key != []:
            log_chng_key = ast.literal_eval(self.inv_dict[dynamodbConstants.LOG_CHNG_INDICATOR_KEY])
        else:
            log_chng_key = []

        if len(log_chng_key) >= 1:
            print(f'Column Util - {self.pipeline_id} - LogIndicatorChangeKey - Available - {log_chng_key}')
            log_chang_key_column = log_chng_key[0]
            if log_chang_key_column not in sorted_cols_dicts_lst and log_chang_key_column in self.read_df.columns:
                query_string += str(f"{log_chang_key_column}" + sep)
                print(f'Column Util - {self.pipeline_id} - LogIndicatorChangeKey is added in the query')
        
        query_string = query_string[:-1]
        full_query = f"select {query_string} from {self.temp_view_name}"
        print(f"Column Util - {self.pipeline_id} - Dynamic sql is as follows: {full_query}")
        return full_query

                                         
        




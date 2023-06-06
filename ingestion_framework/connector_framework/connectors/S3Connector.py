# System Libraries
from datetime import datetime
import ast
import boto3
import csv
# User Libraries
from ingestion_framework.connector_framework.connectors.Connector import abs_connector
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants
from ingestion_framework.utils import common_util
from ingestion_framework.utils import athena_util
from ingestion_framework.utils import logger_util
from ingestion_framework.utils import s3_util
from xml.etree import ElementTree as ET




class S3Connector(abs_connector):

    #@logger_util.common_logger_exception
    def __init__(self, spark, logger, env_type, connector_config):
        self.spark = spark
        self.logger = logger
        self.env_type = env_type
        self.connector_type = connector_config.connector_type
        self.job_start_datetime = connector_config.job_start_datetime
        self.job_end_datetime = connector_config.job_end_datetime
        self.connector_config = connector_config.resource_details
        self.app_name = self.spark.conf.get("spark.app.name")
        self.pipeline_id = self.connector_config[dynamodbConstants.PIPELINE_ID]
        self.partition_column=self.connector_config[dynamodbConstants.TARGET_PARTITION_COL]

    #@logger_util.common_logger_exception
    def open_connection(self):
        pass

    #@logger_util.common_logger_exception
    def close_connection(self):
        pass

    def read_data(self):
        source_environment_details = common_util.get_source_environment_details(self.connector_config, self.env_type)
        print(f"S3Connector - Read data - {self.pipeline_id} - source_details: {source_environment_details}")

        # Building Source File Path from connector config
        source_file_path = f"s3://{source_environment_details[dynamodbConstants.SOURCE_BUCKET_NAME]}" \
                           f"{self.connector_config[dynamodbConstants.SOURCE_FILE_PATH]}" \
                           f"{self.connector_config[dynamodbConstants.SOURCE_FILE_NAME]}"
        print(f"S3Connector - Read data - {self.pipeline_id} - source_file_path: {source_file_path}")

        # Function gets the file and file format
        s3_file_details = self.get_file_extension(source_file_path)
        print(f"S3Connector - Read data - {self.pipeline_id} - s3_file_details: {s3_file_details}")

        if s3_file_details[1] == 'parquet':
            parquet_df = self.spark.read.format('parquet').load(source_file_path)
            print(f"S3Connector - Read data - {self.pipeline_id} - Reading parquet from location: {source_file_path}")
            return parquet_df


        elif s3_file_details[1] == projectConstants.XML:
            root_tag,row_tag=self.extract_root_row_tag(source_environment_details)
            df=self.spark.read.format(projectConstants.XML_PACKAGE)   \
                .option(projectConstants.ROOT_TAG, root_tag)   \
                    .option(projectConstants.ROW_TAG,row_tag)  \
                        .load(source_file_path)
            return df

        elif s3_file_details[1] == projectConstants.XLSX:
            print(f"S3Connector - Read data - connector_config:{self.connector_config}")
            print(f"S3Connector - Read data - XLSX_SHEET_NAME:{self.connector_config[projectConstants.XLSX_SHEET_NAME]}")

            df_xlsx = self.spark.read.format("com.crealytics.spark.excel")\
                               .option("useHeader" , "true")\
                               .option("inferschema" , "false")\
                               .option("dataAddress" , self.connector_config[projectConstants.XLSX_SHEET_NAME]+'!A1')\
                               .load(source_file_path)

            return df_xlsx

        # Function returns a tuple of header(boolean value) and its delimiter
        header_and_delimiter_details = self.detect_file_header_and_delimiter(source_file_path)
        print(f"S3Connector - Read data - {self.pipeline_id} - header_and_delimiter_details: {header_and_delimiter_details}")

        if s3_file_details[1] in projectConstants.SUPPORTED_FLAT_FILE_FORMATS:
            if header_and_delimiter_details[0]:
                print(f"S3Connector - Read data - {self.pipeline_id} - Start of read dataframe for given s3 file path")
                # Spark data frame with a sniffed delimiter and a header
                df = self.spark.read.option("inferSchema", "false").csv(source_file_path,
                                                                       sep=header_and_delimiter_details[1],
                                                                       header=header_and_delimiter_details[0])
                print(f"S3Connector - Read data - {self.pipeline_id} - dataframe created for given s3 file path")
                return df

            if not header_and_delimiter_details[0]:
                # TODO: Work with the lead on defining how to build custom schema if no header
                custom_schema = "define at a later time"
                df = self.spark.read.option("inferSchema", "false").csv(sep=header_and_delimiter_details[1],
                                                                        header=header_and_delimiter_details[0],
                                                                        schema=custom_schema)
                return df
        elif s3_file_details[1] == projectConstants.PIP:
            header = ast.literal_eval(self.connector_config[dynamodbConstants.PIPELINE_COLUMNS])
            sort_header_lst = sorted(header, key=lambda d: int(d[dynamodbConstants.SEQUENCE_NUMBER]))
            src_cols_lst = []

            for nl in sort_header_lst:
                src_cols_lst.append(nl[dynamodbConstants.SOURCE_COLUMN_NAME])
            print(f"S3Connector - Read data - {self.pipeline_id} - Headers are: {src_cols_lst}")
            df = self.spark.read.load(source_file_path, format="csv", sep=header_and_delimiter_details[1],
                                      infer_schema="true", header="false")
            # headers_list = header.split("|")
            new_df = df.toDF(*src_cols_lst)
            return new_df
        else:
            raise Exception(f'Given source file type is not supported. Supported file types are:{projectConstants.SUPPORTED_FLAT_FILE_FORMATS}')


    def write_data(self, df, write_location=projectConstants.DEFAULT_S3_WRITE_LANDING_PREFIX):
        mode = projectConstants.OVERWRITE_MODE

        is_partitioned = False
        if self.connector_config[dynamodbConstants.TARGET_PARTITION_COL]:
            is_partitioned = True

        if is_partitioned == False:
            mode = projectConstants.OVERWRITE_MODE
        
        target_environment_details = common_util.get_target_environment_details(self.connector_config, self.env_type)
        target_bucket_name = target_environment_details[dynamodbConstants.TARGET_BUCKET_NAME]
        target_partition_col = self.connector_config[dynamodbConstants.TARGET_PARTITION_COL]
        target_file_name = self.connector_config[dynamodbConstants.TARGET_FILE_NAME]
        dest_path = "s3a://{}/{}/".format(target_bucket_name, projectConstants.DEFAULT_S3_WRITE_PATH_PREFIX)

        print(f"S3Connector - Write data - {self.pipeline_id} - Write Mode : {mode}")

        #Landing
        if write_location == projectConstants.DEFAULT_S3_WRITE_LANDING_PREFIX:
            print(f"S3Connector - Write data - {self.pipeline_id} - Location : Landing")
            fnlSuffixPath = '{}/{}/'.format(write_location, target_file_name)
            final_path = dest_path + fnlSuffixPath
            df.coalesce(projectConstants.DEFAULT_TARGET_NUM_PARTITIONS).write.format(projectConstants.PARQUET).mode(mode).save(final_path)
        #Final
        else:
            print(f"S3Connector - Write data - {self.pipeline_id} - Location : Final")
            #Target Partition Col - Not Given
            if is_partitioned == False:
                print(f"S3Connector - Write data - {self.pipeline_id} - Is Partitioned : {is_partitioned}")
                fnlSuffixPath = '{}/{}/'.format(write_location, target_file_name)

                # Write to a temp location
                temp_path = dest_path + "tmp/{}/".format(target_file_name)
                df.coalesce(projectConstants.DEFAULT_TARGET_NUM_PARTITIONS).write.format(projectConstants.PARQUET).mode(mode).save(temp_path)
                temp_df = self.spark.read.format(projectConstants.PARQUET).load(temp_path)

                # Write to final location
                final_path = dest_path + fnlSuffixPath
                temp_df.coalesce(projectConstants.DEFAULT_TARGET_NUM_PARTITIONS).write.format(projectConstants.PARQUET).mode(mode).save(final_path)
                
                #Create Athena Table
                athena_util.AthenaUtil.create_athena_raw_table(self.connector_config, final_path, self.env_type, self.app_name, is_partitioned)
            #Target Partition Col - Given
            else:
                print(f"S3Connector - Write data - {self.pipeline_id} - Is Partitioned : {is_partitioned}")
                if self.job_end_datetime != "":
                    dt = self.job_end_datetime.split(' ')[0]
                else:
                    dt = datetime.now().strftime(projectConstants.DATE_FORMAT)
                print(f"S3Connector - Write data - {self.pipeline_id} - Partition Date : {dt}")
                fnlSuffixPath = "{}/{}/{}={}/".format(write_location, target_file_name, target_partition_col, dt)
                athena_table_path_suffix = "{}/{}/".format(write_location, target_file_name)

                # Write to a temp location
                temp_path = dest_path + "tmp/{}/".format(target_file_name)
                df.coalesce(projectConstants.DEFAULT_TARGET_NUM_PARTITIONS).write.format(projectConstants.PARQUET).mode(projectConstants.OVERWRITE_MODE).save(temp_path)
                temp_df = self.spark.read.format(projectConstants.PARQUET).load(temp_path)

                # Write to final location
                final_path = dest_path + fnlSuffixPath
                athena_table_path = dest_path + athena_table_path_suffix

                #Temp: Updating mode to append if partition already exists
                print(f"S3Connector - Write data - {self.pipeline_id} - Target Partition exists already. So changing the write mode to append.")
                mode = projectConstants.APPEND_MODE
                
                temp_df.coalesce(projectConstants.DEFAULT_TARGET_NUM_PARTITIONS).write.format(projectConstants.PARQUET).mode(mode).save(final_path)

                #Create Athena Table
                athena_util.AthenaUtil.create_athena_raw_table(self.connector_config, athena_table_path, self.env_type, self.app_name, is_partitioned)
                athena_util.AthenaUtil.add_partition_athena_raw_table(self.connector_config, athena_table_path, self.env_type, self.app_name)
            
            #S3 life cycle policy
            print(f'S3Connector - Write data - {self.pipeline_id} - S3 Policy Update - Started')
            s3_util.update_s3_life_cycle_policy(self.connector_config, self.env_type)
            print(f'S3Connector - Write data - {self.pipeline_id} - S3 Policy Update - Finished')


    #@logger_util.common_logger_exception
    def get_s3_boto3_client(self):
        s3_client = boto3.client(projectConstants.S3)
        return s3_client


    def get_file_extension(self, s3_file_path):
        """Function to get extension for a given file
        Args:
            s3_file_path: path to an s3 file
        Returns:
            extension of the file path
        """
        file_extension='parquet'
        file_name = s3_file_path.split("/")[-1]
        if "." in file_name:
            file_extension = file_name[file_name.rfind(".") + 1:]
            return file_name, file_extension
        return file_name, file_extension


    def get_s3_path_parts(self, s3_path):
        """Given S3 Path, returns a dictionary with keys "s3_bucket", "s3_key"
        Args:
            s3_path: String representing an S3 path
        Returns:
            Dictionary in the form:
            {
                "s3_bucket": s3_bucket,
                "s3_key": s3_key
            }
        """
        print(f"S3Connector - {self.pipeline_id} - s3_path provided is: {s3_path}")
        if s3_path.startswith("s3://"):
            split = s3_path.split("/")
            s3_bucket = split[2]
            s3_key = "/".join(split[3:])
            print(f"S3Connector - {self.pipeline_id} - split is {split}, s3_bucket is {s3_bucket}, s3_key is {s3_key}")
            return {"s3_bucket": s3_bucket, "s3_key": s3_key}

        if not s3_path.startswith("s3://"):
            print(f"S3Connector - {self.pipeline_id} - Please provide a valid S3 Path:{s3_path}")
            raise Exception(f"S3Connector - {self.pipeline_id} - Please provide a valid S3 Path:{s3_path}")


    def detect_file_header_and_delimiter(self, s3_file_path):
        """Function sniffs for header and delimiter
        Args:
            s3_file_path: path to an s3 file
        Returns:
            has_header boolean and delimiter for given s3_file_path
        """
        s3_path_parts_json = self.get_s3_path_parts(s3_file_path)
        if "*" in s3_path_parts_json["s3_key"]:
            prefix = "/".join(s3_path_parts_json["s3_key"].split("/")[:-1])
            pip_file_type = s3_path_parts_json["s3_key"].split('*')[-1]
            s3_obj_list = self.get_s3_boto3_client().list_objects_v2(Bucket=s3_path_parts_json["s3_bucket"],
                                                                     Prefix=prefix)
            s3_obj = self.get_s3_boto3_client().get_object(Bucket=s3_path_parts_json["s3_bucket"],
                                                           Key=s3_obj_list['Contents'][1]["Key"])
            sample_data = s3_obj['Body'].read().decode('utf-8')
        else:
            s3_obj = self.get_s3_boto3_client().get_object(Bucket=s3_path_parts_json["s3_bucket"],
                                                           Key=s3_path_parts_json["s3_key"])
            sample_data = s3_obj['Body'].read(1024).decode('utf-8')
        has_header = csv.Sniffer().has_header(sample_data)
        dialect = csv.Sniffer().sniff(sample_data)
        file_delimiter = dialect.delimiter
        return has_header, file_delimiter



    @logger_util.common_logger_exception
    def read_tgt_data(self):
        print(f's3connector - {self.pipeline_id} - get data from S3 target')
        # Receive Yaml variables values
        target_environment_details = common_util.get_target_environment_details(self.connector_config, self.env_type)
        print(f's3connector - {self.pipeline_id} the target details are {target_environment_details}')    
        target_bucket_name = target_environment_details[dynamodbConstants.TARGET_BUCKET_NAME]
        file_name = self.connector_config[dynamodbConstants.TARGET_FILE_NAME]
        s3_temp_view=f'{file_name}_s3_tgt_view'
        landingPath = "s3a://{}/{}/".format(target_bucket_name, projectConstants.DEFAULT_S3_WRITE_PATH_PREFIX)
        lndSuffixPath = '{}/{}/'.format(projectConstants.DEFAULT_S3_WRITE_LANDING_PREFIX, file_name)
        fnlSuffixPath = '{}/{}/'.format(projectConstants.DEFAULT_S3_WRITE_FINAL_PREFIX, file_name)
        final_path = '{}/{}/{}/'.format(projectConstants.DEFAULT_S3_WRITE_PATH_PREFIX,
                                        projectConstants.DEFAULT_S3_WRITE_FINAL_PREFIX,
                                        file_name)
        self.path_lnd = landingPath + lndSuffixPath
        self.path_fnl = landingPath + fnlSuffixPath
        print(f's3connector - {self.pipeline_id} - Incremental DF Read from {self.path_lnd} - finished')
        print(f's3connector - {self.pipeline_id} - Checking for Final path - {target_bucket_name}/{final_path}')
        path_exists = s3_util.check_s3_file_exists(target_bucket_name, final_path)
        #Final Path exists
        if path_exists:
            print(f's3connector - {self.pipeline_id} - Final path - {target_bucket_name}/{final_path} - Exists')
            #Compute Full Data DF
            print(f's3connector - {self.pipeline_id} - Full Data DF Read from {self.path_fnl} - started')
            full_data_df = self.spark.read.format(projectConstants.PARQUET).load(self.path_fnl)   
            print(f's3connector - {self.pipeline_id} - Full Data DF Read from {self.path_fnl} - finished')
            if  self.partition_column:
                print(f's3connector - {self.pipeline_id} - dropping the partition column  {self.partition_column} - started')
                full_data_df=full_data_df.drop(f'{self.partition_column}')
                print(f's3connector - {self.pipeline_id} - dropping the partition column  {self.partition_column} - finished')
            else:
                full_data_df=full_data_df
            print(f's3connector - {self.pipeline_id} - Full Data DF count : {full_data_df.count()}')
        else:
            full_data_df=None
        return  full_data_df

    @logger_util.common_logger_exception
    def extract_root_row_tag(self,source_details):
            s3_client=boto3.client(projectConstants.S3)
            source_file_path=f"{self.connector_config[dynamodbConstants.SOURCE_FILE_PATH]}"
            if source_file_path.startswith(projectConstants.FORWARD_SLASH):
                source_file_path=source_file_path[1:]
            if source_file_path.endswith(projectConstants.FORWARD_SLASH):
                pass
            else:
                source_file_path=f"{source_file_path}/"
            source_key=f"{source_file_path}{self.connector_config[dynamodbConstants.SOURCE_FILE_NAME]}"
            response=s3_client.get_object(Bucket=f"{source_details[dynamodbConstants.SOURCE_BUCKET_NAME]}", \
                Key=source_key)
            data=response[projectConstants.BODY].read()
            tree =ET.ElementTree(ET.fromstring(data))
            root = tree.getroot()
            root_tag=root.tag
            row_tag_list=[]
            for child in root:
                if child.tag not in row_tag_list:
                    row_tag_list.append(child.tag)
            if len(row_tag_list)==1:
                row_tag=row_tag_list[0]
            else:
                print(f"S3Connector - {self.pipeline_id} - there are multiple row tags")
                raise Exception(f"S3Connector - {self.pipeline_id} - There are multiple row tags")
            return root_tag,row_tag
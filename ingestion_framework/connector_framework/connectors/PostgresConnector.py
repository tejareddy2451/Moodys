# System Libraries
import sys
import boto3
import base64
import json
import ast
# User Libraries
from ingestion_framework.connector_framework.connectors.Connector import abs_connector
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants
from ingestion_framework.constants import libConstants
from ingestion_framework.utils import logger_util
from ingestion_framework.utils import common_util


class PostgresConnector(abs_connector):
    @logger_util.common_logger_exception
    def __init__(self, spark, logger, env_type, connector_config):
        self.spark = spark
        self.logger = logger
        self.env_type = env_type
        self.connector_type = connector_config.connector_type
        self.connector_config = connector_config.resource_details
        self.target_table_name = self.connector_config[dynamodbConstants.TARGET_TABLE_NAME]
        self.pipeline_id= self.connector_config[dynamodbConstants.PIPELINE_ID]

    #@logger_util.common_logger_exception
    def open_connection(self):
        if self.connector_type == projectConstants.SOURCE_TYPE :
            env_details = common_util.get_source_environment_details(self.connector_config, self.env_type)
        else :
            env_details = common_util.get_target_environment_details(self.connector_config, self.env_type)
        secret_manager_key = env_details[dynamodbConstants.DATABASE_SECRET_MANAGER_NAME]

        client = boto3.client(projectConstants.SEC_MGR)
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_manager_key
        )
        if projectConstants.SEC_STR in get_secret_value_response:
            self.secret = json.loads(get_secret_value_response[projectConstants.SEC_STR])
        else:
            self.secret = json.loads(base64.b64decode(get_secret_value_response[projectConstants.SEC_BIN]))

        self.gcd = {}
        self.gcd[projectConstants.USERNAME] = self.secret[projectConstants.USERNAME]
        self.gcd[projectConstants.PWD] = self.secret[projectConstants.PWD]
        self.gcd[projectConstants.HOST] = self.secret[projectConstants.HOST]
        self.gcd[projectConstants.PORT] = self.secret[projectConstants.PORT]
        self.gcd[projectConstants.DBNAME] = self.secret[projectConstants.DBNAME]

    #@logger_util.common_logger_exception
    def read_data(self):
        # Collect spark read options
        sparkReadOptions = self.get_spark_options(read=True)

        # Perform query given spark read options
        print(f"PostgresConnector {self.pipeline_id} - Reading dataframe from {self.connector_config[dynamodbConstants.SOURCE_DATABASE_TYPE]} database...")
        try:
            return self.spark.read.options(**sparkReadOptions).format(projectConstants.JDBC).load()
        except Exception as err:
            print(f'Postgres Connector  {self.pipeline_id} read_data()- Failed with error: {err}')
            print(f"Postgres Connector  {self.pipeline_id} read_data() Cannot read data from the source...")
            print(f"Check Keys: {list(sparkReadOptions.keys())}")
            raise Exception(f'Postgres Connector  {self.pipeline_id} read_data() - Failed with error: {err}')
            #self.logger.error(sys.exc_info())

    #@logger_util.common_logger_exception
    def write_data(self, df, write_location):
        # Collect spark write options
        sparkWriteOptions = self.get_spark_options(write=True)
        url = sparkWriteOptions[projectConstants.URL]
        del sparkWriteOptions[projectConstants.URL]
        
        target_table_name = self.connector_config[dynamodbConstants.TARGET_TABLE_NAME]

        # Perform write to Postgres given spark write options and dataframe
        print(f"PostgresConnector  - {self.pipeline_id} - write_data()  - Writing dataframe to {self.connector_config[dynamodbConstants.TARGET_TYPE]} database...")
        try:
            df.write.option("truncate", "true").jdbc(url=url, mode=projectConstants.OVERWRITE_MODE, table=target_table_name, properties=sparkWriteOptions)
        except Exception as err:
            print(f"PostgresConnector {self.pipeline_id}  - write_data()  - Cannot write data to database: {self.connector_config[dynamodbConstants.TARGET_TYPE]}...")
            print(f"PostgresConnector {self.pipeline_id}   - write_data()  - Check Keys: {list(sparkWriteOptions.keys())}")
            print(sys.exc_info())
            raise Exception(f'PostgresConnector {self.pipeline_id}  - write_data()  -  Failed with error: {err}')

    @logger_util.common_logger_exception
    def close_connection(self):
        pass

   #@logger_util.common_logger_exception
    def get_spark_options(self, read=False, write=False):
        source_query = self.connector_config[dynamodbConstants.SOURCE_QUERY]
        postgres_host = self.gcd[projectConstants.HOST]
        postgres_port = self.gcd[projectConstants.PORT]
        postgres_dbName = self.gcd[projectConstants.DBNAME]
        sparkOptions = {
            projectConstants.URL: f"""jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_dbName}""",
            projectConstants.USER: self.gcd[projectConstants.USERNAME],
            projectConstants.PWD: self.gcd[projectConstants.PWD]
        }

        if ".rds.amazonaws.com" in postgres_host:
            sparkOptions[projectConstants.SSL] = 'true'
            sparkOptions[projectConstants.SSL_FACTORY_PROPERTY] = projectConstants.SSL_FACTORY_VALUE
        if read:
            sparkOptions[projectConstants.FETCH_SIZE] = projectConstants.DEFAULT_SPARK_JDBC_FETCH_SIZE
            sparkOptions[projectConstants.QUERY] = source_query
        
        if write:
            sparkOptions[projectConstants.DRIVER] =  libConstants.postgresDriver
        
        return sparkOptions

    @logger_util.common_logger_exception
    def read_tgt_data(self):
        query = f"select * from {self.target_table_name}"
        print(f"PostgresConnector - {self.pipeline_id} - read_tgt_data() - Reading dataframe from {self.connector_config[dynamodbConstants.TARGET_DATABASE_TYPE]} database...")
        sparkReadOptions = self.get_spark_options(read=True)
        sparkReadOptions[projectConstants.QUERY] = query
        try:
            return self.spark.read.options(**sparkReadOptions).format(projectConstants.JDBC).load()
        except Exception as err:
            print(f'Postgres Connector - {self.pipeline_id} - read_tgt_data() -  Failed with error: {err}')
            print(f"Postgres Connector  {self.pipeline_id}  - read_tgt_data Cannot read   data from the target table ...")
            print(f"Postgres Connector  {self.pipeline_id}  - read_tgt_data - Check Keys: {list(sparkReadOptions.keys())}")
            raise Exception(f'Postgres Connector  {self.pipeline_id}  - read_tgt_data - Failed with error: {err}')



     

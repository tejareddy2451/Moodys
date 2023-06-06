import sys
import boto3
import base64
import json

from ingestion_framework.connector_framework.connectors.Connector import abs_connector
from ingestion_framework.utils.common_util import *
from ingestion_framework.constants import projectConstants
from ingestion_framework.utils import logger_util
from pyspark.sql.functions import concat_ws, col, udf
from itertools import chain
from pyspark.sql.types import *


class KafkaConnector(abs_connector):

    @logger_util.common_logger_exception
    def __init__(self, spark, logger, env_type, connector_config):
        self.spark = spark
        self.logger = logger
        self.env_type = env_type
        self.connector_config = connector_config.resource_details
        self.target_type = self.connector_config['target_type']
        self.kafka_type = self.connector_config['kafka_type']
        self.topic_name = self.connector_config['topic_name']
        self.auto_offset_reset = self.connector_config['auto_offset_reset']
        self.enable_auto_commit = self.connector_config['enable_auto_commit']
        self.consumer_timeout_ms = self.connector_config['consumer_timeout_ms']

    @logger_util.common_logger_exception
    def open_connection(self):
        source_details = get_source_environment_details(self.connector_config, self.env_type)
        target_details = get_target_environmnet_details(self.connector_config, self.env_type)
        secret_manager_key = target_details["database_secret_manager_name"] if self.target_type == 'kafka' else source_details["database_secret_manager_name"]

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
        self.gcd['bootstrap_servers'] = self.secret['bootstrap_servers']
        self.gcd['sasl_mechanism'] = self.secret['sasl_mechanism']
        self.gcd['security_protocol'] = self.secret['security_protocol']

    @logger_util.common_logger_exception
    def read_data(self):
        # Collect spark read options
        sparkReadOptions = self.get_spark_options(consumer=True)
        # Perform query given spark read options
        print(f"KafkaConnector - Reading {self.kafka_type} dataframe from kafka topic {self.topic_name}...")
        try:
            return self.spark.read.format("kafka").options(**sparkReadOptions).load()
        except Exception as err:
            self.logger.error(f'Kafka Connector - Read Failed with error: {err}')
            self.logger.error(f"Check Keys: {list(sparkReadOptions.keys())}")
            self.logger.error(sys.exc_info())

    def concat(self, type):
        def concat_(*args):
            return list(chain(*args))
        return udf(concat_, ArrayType(type))

    @logger_util.common_logger_exception
    def write_data(self, df, write_location, is_partitioned):
        # Collect spark write options
        sparkWriteOptions = self.get_spark_options(producer=True)
        concat_string_arrays = self.concat(StringType())
        # Perform write to kafka topic given spark write options and dataframe
        print(f"KafkaConnector - Writing dataframe to {self.kafka_type} kafka topic {self.topic_name}...")
        try:
            print(f"KafkaConnector - Provided config:{sparkWriteOptions}")
            df = df.select(concat_ws("|", concat_string_arrays(*df.columns)).alias('value'))
            df.write.format("kafka").options(**sparkWriteOptions).save()
            # .selectExpr("CAST(payment_id AS STRING) AS key", "to_json(struct(*)) AS value") \
        except Exception as err:
            self.logger.error(f'Kafka Connector - Write Failed with error: {err}')
            self.logger.error(f"Check Keys: {list(sparkWriteOptions.keys())}")
            self.logger.error(sys.exc_info())

    @logger_util.common_logger_exception
    def close_connection(self):
        pass

    @logger_util.common_logger_exception
    def get_spark_options(self, consumer=False, producer=False):
        spark_options = {}
        if consumer:
            spark_options = {
                "kafka.bootstrap.servers": self.gcd['bootstrap_servers'],
                "sasl_plain_username": self.gcd[projectConstants.USERNAME],
                "sasl_plain_password": self.gcd[projectConstants.PWD],
                "subscribe": self.topic_name,
                "startingOffsets": self.auto_offset_reset,
                "endingOffsets": "latest",
                "kafka.security.protocol": self.gcd['security_protocol'],
                # "kafka.sasl.mechanism": self.secret['sasl_mechanism'],
                "kafka.sasl.mechanism": "AWS_MSK_IAM",
                "kafka.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
                "kafka.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
            }

        if producer:
            spark_options = {
                "kafka.bootstrap.servers": self.gcd['bootstrap_servers'],
                "sasl_plain_username": self.gcd[projectConstants.USERNAME],
                "sasl_plain_password": self.gcd[projectConstants.PWD],
                "topic": self.topic_name,
                "startingOffsets": self.auto_offset_reset,
                "endingOffsets": "latest",
                "kafka.security.protocol": self.gcd['security_protocol'],
                "kafka.sasl.mechanism": self.secret['sasl_mechanism'],
            }

        return spark_options

    @logger_util.common_logger_exception
    def read_tgt_data(self):
        return None

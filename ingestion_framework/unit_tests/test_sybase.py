"""
from ConnectionFactory.src.com.moodys.inventoryReader import InventoryReader
from ConnectionFactory.src.com.moodys.connectorSupplier import ConnectorSupplier

import logging

def main():

    logger = logging.getLogger('Connection Factory Logger')

    #sybase example inv_id
    inv_dict = InventoryReader.get_inventory_dict_from_yaml('sbi.yaml','3877b605-195c-3918-a465-35f89927acf0')

    src_dict=InventoryReader(inv_dict).get_source_connector_config()
    tgt_dict=InventoryReader(inv_dict).get_target_connector_config()

    spark = None

    src_connector=ConnectorSupplier(spark, logger).get_connector(src_dict)
    tgt_connector=ConnectorSupplier(spark, logger).get_connector(tgt_dict)
    
    df = src_connector.read_data()
    tgt_connector.write_data(df)


if __name__ == "__main__":
    main()

"""

"""
Main Driver Module for ETL ingestion process.
Script will be executed from aws glue.
"""
import sys
#from ingestion_framework.commons import *
from ingestion_framework.utils import *
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants
from ingestion_framework.scripts import job_initializer
from ingestion_framework.connector_framework.connector_utils.InventoryReader import InventoryReader
from ingestion_framework.connector_framework.connector_utils.ConnectorSupplier import ConnectorSupplier
from ingestion_framework.utils.scd_util import SCD

from awsglue.utils import getResolvedOptions
import boto3
from botocore.exceptions import ClientError
 
def mis_dm_ingfw_appRunner():
    try:
        # Initializing spark and Logger
        runTimeArgs = getResolvedOptions(sys.argv, [projectConstants.JOBNAME, projectConstants.APP_NAME,projectConstants.CRAWLER_NAME])
        app_name = runTimeArgs[projectConstants.APP_NAME]
        crawler_name = runTimeArgs[projectConstants.CRAWLER_NAME]
        job_env = runTimeArgs[projectConstants.JOBNAME].split('-')[-1]
        spark = job_initializer.initializeSpark(app_name)
        logger = job_initializer.getGlueLogger(spark)
        logger.info('AppRunner - Initialized Spark Session & logger')

        #sybase example inv_id
        inv_dict = InventoryReader.get_inventory_dict_from_yaml('marketing_test.yaml','8d3933a0-b0ca-4fd1-8405-b6d4cb885380')

        src_dict=InventoryReader(inv_dict).get_source_connector_config()
        tgt_dict=InventoryReader(inv_dict).get_target_connector_config()

        src_connector=ConnectorSupplier(spark, logger).get_connector(src_dict)
        tgt_connector=ConnectorSupplier(spark, logger).get_connector(tgt_dict)

        # Read all datasources, Store in Dataframe
        source_type = src_dict.resource_details[dynamodbConstants.SOURCE_TYPE]
        df = src_connector.read_data()
        logger.info(f'AppRunner - loaded dataframe for source {source_type}')

        # Write all the datasources to respective Landing Folders
        tgt_connector.write_data(df)
        logger.info(f'AppRunner - Wrote dataframe for source {source_type}')

        # Apply SCD procedure
        configParams = None  # TODO need resolve SCD procedure code with new connection factory code
        SCD.scd_process(spark, logger, src_dict, app_name, job_env)
        #scd_process(spark, logger, src_dict, app_name, job_env)
        logger.info('AppRunner - SCD procedure complete')
        # Run Crawler
        response= start_a_crawler(crawler_name)
        logger.info(f'Crawler function executed with response : {response}')

    except Exception as err:
        logger.error(f"Exception Occured | Module:AppRunner_Job.py | Error:{str(err)}")
        logger.error(sys.exc_info())

def start_a_crawler(crawler_name):
   session = boto3.session.Session()
   glue_client = session.client(projectConstants.GLUE)
   try:
      response = glue_client.start_crawler(Name=crawler_name)
      return response

   except ClientError as err:
      raise Exception(f"BOTO3 Client Error in start_a_crawler: {str(err)}")

   except Exception as err:
      raise Exception(f"Unexpected Error in start_a_crawler: {str(err)}" )


if __name__ == '__main__':
    mis_dm_ingfw_appRunner()





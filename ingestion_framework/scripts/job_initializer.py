from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from ingestion_framework.constants import projectConstants
from ingestion_framework.utils import logger_util


#@logger_util.common_logger_exception
def initializeSpark(appName): 
    spark = SparkSession\
        .builder\
        .appName(appName)\
        .config('spark.driver.extraClassPath', projectConstants.DEFAULT_SPARK_EXTRA_CLASS_PATH_CONFIG)\
        .config('spark.executor.extraClassPath', projectConstants.DEFAULT_SPARK_EXTRA_CLASS_PATH_CONFIG) \
        .getOrCreate()
    return spark

#@logger_util.common_logger_exception
def getGlueLogger(sparkSession):
    glueContext = GlueContext(sparkSession.sparkContext)
    return glueContext.get_logger()

from ..constants import libConstants

#Path constants
DEFAULT_JARS_PATH = '/code/jars/'
DEFAULT_S3_WRITE_PATH_PREFIX = 'data'
DEFAULT_S3_WRITE_LANDING_PREFIX = 'landing'
DEFAULT_S3_WRITE_FINAL_PREFIX = 'final'
DEFAULT_S3_YAML ='config/yaml/'


#JOB PARAMETERS
JOB_NAME='JOB_NAME'
APP_NAME = 'AppName'
DOMAIN_NAME = 'DomainName'
AWS_REGION = 'AWSRegion'
PIPELINE_ID = 'PipelineId'
JOB_ENVIRONMENT = 'JobEnvironment'
RUN_ALL = 'RunAllPipelines'
VERSION = 'Version'
JOB_START_DATETIME = 'JobStartDateTime'
JOB_END_DATETIME = 'JobEndDateTime'
BUCKET_NAME = 'BucketName'
PATH_TO_PURGE = 'Directory'
NUM_DAYS = 'NumberOfDays'
RUN_IN_DAILY_CHUNKS = 'RunInDailyChunks'
SEQUENCE_RUN='RunPipelinesInSequence'


#Secret Manager
SEC_MGR = 'secretsmanager'
SEC_STR = 'SecretString'
SEC_BIN = 'SecretBinary'
USER = 'user'
USERNAME = 'username'
PWD = 'password'
HOST = 'host'
PORT = 'port'
DBNAME = 'dbname'
REQUIRE = 'require'
URL = 'url'
DRIVER = 'driver'
FETCH_SIZE = 'fetch_size'
SSL ='ssl'
SSL_MODE = 'sslmode'
SSL_FACTORY_PROPERTY = 'sslfactory'
SSL_FACTORY_VALUE = 'org.postgresql.ssl.NonValidatingFactory'
JDBC = 'jdbc'


#spark constants
DEFAULT_SPARK_JDBC_FETCH_SIZE = '500000'
DEFAULT_TARGET_NUM_PARTITIONS = 1
DEFAULT_SPARK_EXTRA_CLASS_PATH_CONFIG = DEFAULT_JARS_PATH+libConstants.oracleJar+':'+DEFAULT_JARS_PATH+libConstants.sybaseJar+':'+DEFAULT_JARS_PATH+libConstants.db2Jar+':'+DEFAULT_JARS_PATH+libConstants.postgresJar+':'+DEFAULT_JARS_PATH+libConstants.mysqlJar
WRITE_MODE = 'mode'
OVERWRITE_MODE = 'overwrite'
APPEND_MODE = 'append'
DBTABLE = 'dbtable'
QUERY = 'query'
PARTITION_COLUMN = 'partitionColumn'
LOWER_BOUND = 'lowerBound'
UPPER_BOUND = 'upperBound'
NUM_PARTITIONS = 'numPartitions'
SPARK_TRUE = 'True'
DEFAULT_ORACLE_PARTITION_COLUMN='record_key'
DEFAULT_ORACLE_PARTITIONS=5
UPSERT_BATCH_SIZE = 1000


#Parser code constants
ACCOUNT = 'Account'
PARSER = 'parser'
DEFAULT = 'default'
ENCODER = 'Encoder'
MAX_VERSION_NUMBER = 'max_version_number'
P_ID = 'p_id'
V_NO = 'v_no'


#Datatype Constants
STRING = "string"


#File Formats
YAML = 'yaml'
CSV = 'csv'
TXT = 'txt'
PARQUET = 'parquet'
SUPPORTED_FLAT_FILE_FORMATS = [CSV, TXT, PARQUET]
PIP = 'PIP'
XML='xml'
XLSX='xlsx'
XLSX_SHEET_NAME = 'sheet_name'

#AWS Constants
GLUE = 'glue'
S3 = 's3'
ATHENA = 'athena'
STS = 'sts'
US_EAST_1='us-east-1'
US_WEST_2='us-west-2'


#Python Constants
NONE = 'None'
INNER = 'inner'


#Code Constants
DATE_FORMAT = '%Y-%m-%d'
ETL_INSERT_TS = 'etl_insert_ts'
STARTED_FLAG = 'STARTED'
IN_PROGRESS_FLAG = 'IN_PROGRESS'
SUCCESS_FLAG = 'SUCCESS'
FAILURE_FLAG = 'FAILURE'
SOURCE_TYPE = 'source'
TARGET_TYPE = 'target'
FULL_LOAD = 'full'
DELTA_LOAD = 'delta'

ORACLE_DB = 'oracle'
POSTGRES_DB = 'postgres'

CDC_ESG_UPDATE = 'UPD'
CDC_ESG_INSERT = 'INS'
CDC_ESG_DELETE = 'DEL'

CDC_SUPPORTED_UPDATES = [CDC_ESG_UPDATE]
CDC_SUPPORTED_INSERTS = [CDC_ESG_INSERT]
CDC_SUPPORTED_DELETES = [CDC_ESG_DELETE]

#HTTP STATUS CODES
HTTP_SUCCESS = '200'

#XML CONSTANTS
ROOT_TAG="rootTag"
ROW_TAG="rowtag"
XML_PACKAGE="com.databricks.spark.xml"
BODY="Body"
FORWARD_SLASH="/"

# COLLIBRA CONSTANTS
COLLIBRA_TOKEN='CollibraToken'
JOB_MONITORING_URL='JobMonitoringURL'
COLLIBRA = 'collibra'
COLLIBRA_URL = 'CollibraURL'

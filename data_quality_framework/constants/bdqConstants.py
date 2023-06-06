#DYNAMODB
DYNAMO = 'dynamodb'
LAST_EVAL_KEY = 'LastEvaluatedKey'
ITEMS = 'Items'

#DYNAMO TABLES
BDQ_INVENTORY_TABLE = 'datamesh-inventory-bdq'
BDQ_RULES_TABLE = 'datamesh-bdq-rules-mapping'
BDQ_TRACKER_TABLE = 'datamesh-bdq-tracker'

#DYNAMO COLUMNS
RULE_NAME = 'rule_name'
COLUMN_NAME = 'column'
USER_VALUES = 'user_values'
STORAGE_TYP = 'storage_type'
DATA_TABLE_NAME = 'table_name'
APP_NAME = 'app_name'
RULE_ID = 'rule_id'
SCHEMA_NAME = 'schema_name'
DOMAIN_NAME = 'domain_name'
ZONE = 'zone'
KEY_COLUMNS = 'key_columns'
QUERY = 'query'
EXP_STMT = 'expectation_statement'
EXP_METHOD_ARGS = 'expectation_method_args'
USER_VALS_LIST = 'user_vals_list'
USER_VALS_TUPLE = 'user_vals_tuple'
USER_COL_VAL = 'user_col_val'
USER_COL_VALS = 'user_col_vals'
ACTIVE_FLAG = 'active_flag'
RULE_DESCRIPTION = 'rule_description'

#TRACKER STATUS
SUCCESS = 'SUCCESS'
FAILED = 'FAILED'

#STATIC PATHS
VALIDATIONS = 'validations'
EXPECTATIONS = 'expectations'
CHECKPOINTS = 'checkpoints'
DATA_DOCS = 'data_docs'
ERROR_PATH = 'data/error/'

#DEFAULTS
PURGE_DAYS = '15'
ACCOUNT = 'Account'
STS = 'sts'
FRAMEWORK = 'datamesh-bdq'

#S3 BUCKETS
GE_BUCKET = 'datamesh-datadocs'

#SNS
SNS = 'sns'
JSON= 'json'
SNS_ARN_PREFIX = 'arn:aws:sns'

#ERROR FILE HEADERS
ERROR_APP_NM = 'app_name'
EXEC_ID = 'Execution ID'
DOMAIN_NM = 'Domain Name'
ERROR_ZONE = 'Zone'
ERROR_RULE_ID = 'rule_id'
ERROR_RULE_NM = 'Rule Name'
DB_SCHEMA = 'Database/schema'
ERROR_TABLE_NM = 'Table Name'
ERROR_PRIM_KEYS = 'Failed_Record_Keys'
RUN_DT = 'run_dt'

#ERROR TABLE COLUMNS
ATHENA_TABLE_NM = 'bdq_error'
COL_EXEC_ID = 'execution_id'
COL_PRIM_KEYS = 'primary_keys'
COL_DB_NM = 'db_name'
COL_TYPE_100 = 'VARCHAR(100)'
COL_TYPE_250 = 'VARCHAR(250)'
COL_TYPE_50 = 'VARCHAR(50)'
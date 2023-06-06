from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig
from data_quality_framework.constants import bdqConstants, projectConstants
import boto3
import json

def postgres_datacontext(data_source_name, schema_name, bucket_name, env_name):
    client_secrets = json.loads(
        boto3.client(projectConstants.SEC_MGR).get_secret_value(SecretId=f"datamesh/{env_name}/referencedata/postgres").get(
            projectConstants.SEC_STR))
    return DataContextConfig(datasources = {
                      f"{data_source_name}": DatasourceConfig(
                          class_name="Datasource",
                          execution_engine={
                              "class_name": "SqlAlchemyExecutionEngine",
                              "credentials": {
                                  "drivername": "postgresql+psycopg2",
                                  "host": client_secrets[projectConstants.HOST],
                                  "port": client_secrets[projectConstants.PORT],
                                  "username": client_secrets[projectConstants.USERNAME],
                                  "password": client_secrets[projectConstants.PWD],
                                  "database": client_secrets[projectConstants.DBNAME],
                                  "schema_name": f"{schema_name}"
                              },
                          },
                          data_connectors={
                              "default_runtime_data_connector_name": {
                                  "class_name": "RuntimeDataConnector",
                                  "batch_identifiers": ["default_identifier_name"],
                              },
                              "default_inferred_data_connector_name": {
                                  "class_name": "InferredAssetSqlDataConnector",
                                  "name": f"{data_source_name}",
                              },
                          }
                      )
                  },
    stores = {
                 "expectations_S3_store": {
                     "class_name": "ExpectationsStore",
                     "store_backend": {
                         "class_name": "TupleS3StoreBackend",
                         "bucket": f"{bucket_name}",
                         "prefix": f"{data_source_name}/{bdqConstants.EXPECTATIONS}",
                     },
                 },
                 "validations_S3_store": {
                     "class_name": "ValidationsStore",
                     "store_backend": {
                         "class_name": "TupleS3StoreBackend",
                         "bucket": f"{bucket_name}",
                         "prefix": f"{data_source_name}/{bdqConstants.VALIDATIONS}",
                     },
                 },
                 "checkpoint_S3_store": {
                     "class_name": "CheckpointStore",
                     "store_backend": {
                         "class_name": "TupleS3StoreBackend",
                         "bucket": f"{bucket_name}",
                         "prefix": f"{data_source_name}/{bdqConstants.CHECKPOINTS}",
                     },
                 },
                 "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
             },
    expectations_store_name = "expectations_S3_store",
    validations_store_name = "validations_S3_store",
    evaluation_parameter_store_name = "evaluation_parameter_store",
    checkpoint_store_name = "checkpoint_S3_store",
    data_docs_sites = {
                          "s3_site": {
                              "class_name": "SiteBuilder",
                              "store_backend": {
                                  "class_name": "TupleS3StoreBackend",
                                  "bucket": f"{bucket_name}",
                                  "prefix": f"{data_source_name}/{bdqConstants.DATA_DOCS}",
                              },
                              "site_index_builder": {
                                  "class_name": "DefaultSiteIndexBuilder",
                                  "show_cta_footer": True,
                              },
                          }
                      },
    validation_operators = {
                               "action_list_operator": {
                                   "class_name": "ActionListValidationOperator",
                                   "action_list": [
                                       {
                                           "name": "store_validation_result",
                                           "action": {"class_name": "StoreValidationResultAction"},
                                       },
                                       {
                                           "name": "store_evaluation_params",
                                           "action": {"class_name": "StoreEvaluationParametersAction"},
                                       },
                                       {
                                           "name": "update_data_docs",
                                           "action": {"class_name": "UpdateDataDocsAction"},
                                       },
                                   ],
                               }
                           },
    anonymous_usage_statistics = {
        "enabled": True
    })
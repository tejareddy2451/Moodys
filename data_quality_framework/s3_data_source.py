from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig
from data_quality_framework.constants import bdqConstants

def s3_datacontext(data_source_name, bucket_name):

    return DataContextConfig(datasources={
        f"{data_source_name}": DatasourceConfig(
            class_name="Datasource",
            execution_engine={
                "class_name": "PandasExecutionEngine",
            },
            data_connectors={
                "default_runtime_data_connector_name": {
                    "class_name": "RuntimeDataConnector",
                    "batch_identifiers": ["runtime_batch_identifier_name"],
                },
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetFilesystemDataConnector",
                    "name": f"{data_source_name}",
                    "base_directory": "/tmp",
                    "default_regex":{"group_names":["data_asset_name"], "pattern": "(.*)"}
                },
            }
        )
    },
    stores={
        "expectations_S3_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": f"{bucket_name}",
                "prefix": "great_expectations/expectations",
            },
        },
        "validations_S3_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": f"{bucket_name}",
                "prefix": "great_expectations/validations",
            },
        },
        "checkpoint_S3_store": {
            "class_name": "CheckpointStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": f"{bucket_name}",
                "prefix": "great_expectations/checkpoints",
            },
        },
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
    },
    expectations_store_name="expectations_S3_store",
    validations_store_name="validations_S3_store",
    evaluation_parameter_store_name="evaluation_parameter_store",
    checkpoint_store_name="checkpoint_S3_store",
    data_docs_sites={
        "s3_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": f"{bucket_name}",
                "prefix": "great_expectations/data_docs",
            },
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
        }
    },
    validation_operators={
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
    anonymous_usage_statistics={
        "enabled": True
    }
    )
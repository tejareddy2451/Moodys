import yaml
from ingestion_framework.connector_framework.connector_utils.ConnectorConfig import ConnectorConfig
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants
from ingestion_framework.utils import logger_util


class InventoryReader :
    #@logger_util.common_logger_exception
    def __init__(self, inventory_dic):
        self.inventory_dict = inventory_dic

    #@logger_util.common_logger_exception
    def get_source_connector_config(self, job_start_datetime, job_end_datetime):
        if self.inventory_dict[dynamodbConstants.SOURCE_TYPE] == dynamodbConstants.DATABASE:
            sourceName = self.inventory_dict[dynamodbConstants.SOURCE_DATABASE_TYPE]
        else:
            sourceName = projectConstants.S3
        return ConnectorConfig(projectConstants.SOURCE_TYPE, sourceName, self.inventory_dict, job_start_datetime, job_end_datetime).getConfig()
        
    #@logger_util.common_logger_exception
    def get_target_connector_config(self, job_start_datetime, job_end_datetime):
        if self.inventory_dict[dynamodbConstants.TARGET_TYPE] == dynamodbConstants.DATABASE:
            targetName = self.inventory_dict[dynamodbConstants.TARGET_DATABASE_TYPE]
        else:
            targetName = projectConstants.S3
        return ConnectorConfig(projectConstants.TARGET_TYPE, targetName, self.inventory_dict, job_start_datetime, job_end_datetime).getConfig()
    
    #@logger_util.common_logger_exception
    def get_pipeline_dict_from_yaml(app_name, pipeline_id, version):
        configFileName = f'{app_name}.{projectConstants.YAML}'
        with open(configFileName, "r") as config_file:
            config_dict = yaml.load(config_file, Loader=yaml.SafeLoader)
        return next((source for source in config_dict[dynamodbConstants.PIPELINES] if source[dynamodbConstants.PIPELINE_ID] == pipeline_id and source[dynamodbConstants.VERSION_NUMBER] == version), None)
    
    #@logger_util.common_logger_exception
    def get_all_pipelines_from_yaml(app_name):
        configFileName = f'{app_name}.{projectConstants.YAML}'
        with open(configFileName, "r") as config_file:
            config_dict = yaml.load(config_file, Loader=yaml.SafeLoader)
        return config_dict[dynamodbConstants.PIPELINES]

    #@logger_util.common_logger_exception
    def get_all_pipelineids_with_latest_versions(app_name):
        pipelines = InventoryReader.get_all_pipelines_from_yaml(app_name)
        pipelines_with_versions = {}
        for pipeline in pipelines:
            if pipeline[dynamodbConstants.PIPELINE_ID] in pipelines_with_versions:
                if pipeline[dynamodbConstants.ACTIVE] == '' or pipeline[dynamodbConstants.ACTIVE] == None :
                    pipeline[dynamodbConstants.ACTIVE] = True
                if pipeline[dynamodbConstants.ACTIVE] and pipeline[dynamodbConstants.VERSION_NUMBER] > pipelines_with_versions[pipeline[dynamodbConstants.PIPELINE_ID]]:
                    pipelines_with_versions.update({pipeline[dynamodbConstants.PIPELINE_ID] : pipeline[dynamodbConstants.VERSION_NUMBER]})
            else:
                if pipeline[dynamodbConstants.ACTIVE] == '' or pipeline[dynamodbConstants.ACTIVE] == None :
                    pipeline[dynamodbConstants.ACTIVE] = True
                if pipeline[dynamodbConstants.ACTIVE] :
                    pipelines_with_versions[pipeline[dynamodbConstants.PIPELINE_ID]] = pipeline[dynamodbConstants.VERSION_NUMBER]
        
        return pipelines_with_versions
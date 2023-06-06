from ingestion_framework.utils import logger_util


class ConnectorConfig :    
    #@logger_util.common_logger_exception
    def __init__(self, connector_type, connector_name, resource_details, job_start_datetime, job_end_datetime):
        self.connector_type = connector_type
        self.connector_name = connector_name
        self.resource_details = resource_details
        self.job_start_datetime = job_start_datetime
        self.job_end_datetime = job_end_datetime

    #@logger_util.common_logger_exception
    def getConfig(self):
        return self

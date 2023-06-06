# User Libraries
from ingestion_framework.utils import logger_util


class ConnectorSupplier:
    
    #@logger_util.common_logger_exception
    def __init__(self, spark, logger, env_type):
        self.spark = spark
        self.logger = logger
        self.env_type = env_type

    #@logger_util.common_logger_exception
    def get_connector(self, connector_config):
        connector_type = connector_config.connector_type
        connector_name = connector_config.connector_name
        resource_details = connector_config.resource_details
        connector_class_name = connector_name.title() + "Connector"
        connector_path = f"ingestion_framework.connector_framework.connectors.{connector_class_name}"
        print(f"ConnectorSupplier - connector path: {connector_path}")
        connector_module = __import__(connector_path)
        print(f"ConnectorSupplier - connector module after __import__: {connector_module}")
        connector_class = getattr(connector_module, connector_class_name)
        print(f"ConnectorSupplier - connector class: {connector_class}")
        connector = connector_class(self.spark, self.logger, self.env_type, connector_config)
        connector.open_connection()
        return connector
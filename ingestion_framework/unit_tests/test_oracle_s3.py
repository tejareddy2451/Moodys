from ingestion_framework.connector_framework.connector_utils import InventoryReader
from ingestion_framework.connector_framework.connector_utils import ConnectorSupplier
from ingestion_framework.scripts.job_initializer import *

import logging

def main():

    logger = logging.getLogger('Connection Factory Logger')

    #sybase example inv_id
    inv_dict = InventoryReader.get_inventory_dict_from_yaml('sbi.yaml','80666eff-7048-31ff-84f0-5b3a04bb6421')
    print(inv_dict)
    print('trial')

    src_dict=InventoryReader(inv_dict).get_source_connector_config()
    tgt_dict=InventoryReader(inv_dict).get_target_connector_config()

    spark = initializeSpark('sbi')

    src_connector=ConnectorSupplier(spark, logger).get_connector(src_dict)
    tgt_connector=ConnectorSupplier(spark, logger).get_connector(tgt_dict)
    
    df = src_connector.read_data()
    print(df)
    tgt_connector.write_data(df)


if __name__ == "__main__":
    main()
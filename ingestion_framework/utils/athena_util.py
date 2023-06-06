import boto3
import ast
from ingestion_framework.constants import projectConstants
from ingestion_framework.constants import dynamodbConstants


class AthenaUtil:

    @staticmethod
    def add_partition_athena_raw_table(conf, data_path, env, app_name):
        pipeline_id = conf[dynamodbConstants.PIPELINE_ID]
        print(f'AthenaUtil - add_partition_athena_raw_table - {pipeline_id} - Started')

        athena_us_east_1_client = boto3.client(projectConstants.ATHENA, region_name=projectConstants.US_EAST_1)
        athena_us_west_2_client  = boto3.client(projectConstants.ATHENA, region_name=projectConstants.US_WEST_2)
        client_list=[athena_us_east_1_client,athena_us_west_2_client]
        s3_path = data_path[6:]
        bucket_name = s3_path.split('/')[0]

        db_name = "datamesh_{}_raw_{}".format(conf[dynamodbConstants.DOMAIN_NAME], env)
        work_grp = "datamesh-{}-{}".format(app_name, env)
        table_name = conf[dynamodbConstants.TARGET_FILE_NAME]
        out_loc = "s3://{}/logs/athena/{}/".format(bucket_name, table_name)
        qry = "MSCK REPAIR TABLE {}".format(table_name)
        context = {'Database': db_name}

        print(f'AthenaUtil - add_partition_athena_raw_table - {pipeline_id} - Executing the query - {qry} - Started')
        for client in client_list:
            res = client.start_query_execution(
                QueryString=qry,
                QueryExecutionContext=context,
                WorkGroup=work_grp,
                ResultConfiguration={'OutputLocation': out_loc})
            q_exec_id = res['QueryExecutionId']
            
            while True:
                status = client.get_query_execution(QueryExecutionId=q_exec_id)
                state = status['QueryExecution']['Status']['State']
                if state not in ["RUNNING", "QUEUED", "CANCELED"]:
                    break
            if state == "FAILED":
                error_message = status['QueryExecution']['Status']['StateChangeReason']
                print(f'AthenaUtil - add_partition_athena_raw_table - {pipeline_id} - Executing the query - Failed')
                print(f'AthenaUtil - add_partition_athena_raw_table - {pipeline_id} - Error - {error_message}')
                raise Exception(f"Athena query execution failed with Error for table {pipeline_id} : {error_message}")
            else:
                print(f'AthenaUtil - add_partition_athena_raw_table - {pipeline_id} - Executing the query - Succeeded')
        print(f'AthenaUtil - add_partition_athena_raw_table - {pipeline_id} - Executing the query - Finished')


    @staticmethod
    def create_athena_raw_table(conf, data_path, env, app_name, is_partitioned=False):
        pipeline_id = conf[dynamodbConstants.PIPELINE_ID]
        print(f'AthenaUtil - create_athena_raw_table - {pipeline_id} - Started')
        print(f'AthenaUtil - create_athena_raw_table - {pipeline_id} - in regions {projectConstants.US_EAST_1} and {projectConstants.US_WEST_2}')
        athena_us_east_1_client = boto3.client(projectConstants.ATHENA, region_name=projectConstants.US_EAST_1)
        athena_us_west_2_client  = boto3.client(projectConstants.ATHENA, region_name=projectConstants.US_WEST_2)
        cols = conf[dynamodbConstants.PIPELINE_COLUMNS]
        lst = ast.literal_eval(cols)
        sort_lst = sorted(lst, key=lambda d: int(d[dynamodbConstants.SEQUENCE_NUMBER]))
        client_list=[athena_us_east_1_client,athena_us_west_2_client]

        src_cols_lst = []
        tgt_col_lst = []
        tgt_data_type = []
        tgt_col_len = []
        tgt_col_prec = []

        for nl in sort_lst:
            src_cols_lst.append(nl[dynamodbConstants.SOURCE_COLUMN_NAME])
            tgt_col_lst.append(nl[dynamodbConstants.TARGET_COLUMN_NAME])
            tgt_data_type.append(nl[dynamodbConstants.TARGET_DATATYPE])
            tgt_col_len.append(nl.get(dynamodbConstants.TARGET_LENGTH, 0))
            tgt_col_prec.append(nl.get(dynamodbConstants.TARGET_PRECISION, 0))

        cor_type_lst = []
        for index in range(len(tgt_data_type)):
            if tgt_data_type[index] == 'DATETIME' or tgt_data_type[index] == 'datetime':
                cor_type_lst.append('TIMESTAMP')
            elif tgt_data_type[index] == 'STRING' and int(tgt_col_len[index]) != 0:
                cor_type_lst.append('VARCHAR')
            else:
                cor_type_lst.append(tgt_data_type[index])

        s3_path = data_path[6:]
        bucket_name = s3_path.split('/')[0]

        db_name = "datamesh_{}_raw_{}".format(conf[dynamodbConstants.DOMAIN_NAME], env)
        work_grp = "datamesh-{}-{}".format(app_name, env)

        table_name = conf[dynamodbConstants.TARGET_FILE_NAME]
        out_loc = "s3://{}/logs/athena/{}/".format(bucket_name, table_name)
        table_data_path = data_path.replace("s3a://", "s3://")
        context = {'Database': db_name}
        drop_table_query = f"DROP TABLE IF EXISTS `{table_name}`;"
        print(f'AthenaUtil - drop_athena_raw_table - {pipeline_id} - syntax - {drop_table_query}') 

        q_1 = """create external table IF NOT EXISTS `{}`
        (
        """.format(table_name)

        q_2 = """"""

        for index in range(len(tgt_data_type)):
            if tgt_col_len[index] != 0 and tgt_col_prec[index] != 0:
                tmp = "`{}`   {}({},{}),\n".format(tgt_col_lst[index], cor_type_lst[index], tgt_col_len[index], tgt_col_prec[index])
            elif cor_type_lst[index].lower() == "decimal":
                if tgt_col_len[index] == 0:
                    tgt_col_len[index] = 38
                tmp = "`{}`   {}({},{}),\n".format(tgt_col_lst[index], cor_type_lst[index], tgt_col_len[index], tgt_col_prec[index])
            elif cor_type_lst[index].lower() == "char" or cor_type_lst[index].lower() == "varchar":
                tmp = "`{}`   {}({}),\n".format(tgt_col_lst[index], cor_type_lst[index], tgt_col_len[index])
            else:
                tmp = "`{}`   {},\n".format(tgt_col_lst[index], cor_type_lst[index])
            q_2 = q_2 + tmp

        q_2 = q_2[:-2]

        if is_partitioned == False:
            q_3 = """)
            STORED AS PARQUET
            LOCATION '{}/'
            """.format(table_data_path)
        else:
            q_3 = """)
            PARTITIONED BY ({} string)
            STORED AS PARQUET
            LOCATION '{}/'
            """.format(conf[dynamodbConstants.TARGET_PARTITION_COL], table_data_path)

        create_table_query =  q_1 + q_2 + q_3
        athena_drop_create_list=[drop_table_query,create_table_query]

        for client in client_list:
            region_name=client.meta.region_name
            print(f'AthenaUtil - create_athena_raw_table - {pipeline_id} -in aws region - {region_name} - Started')
            for query in athena_drop_create_list:
                print(f'AthenaUtil - create_athena_raw_table - {pipeline_id} - Executing the query - {query} - Started')
                res = client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext=context,WorkGroup=work_grp,
                    ResultConfiguration={'OutputLocation': out_loc})
                q_exec_id = res['QueryExecutionId']
                
                while True:
                    status = client.get_query_execution(QueryExecutionId=q_exec_id)
                    state = status['QueryExecution']['Status']['State']
                    if state not in ["RUNNING", "QUEUED", "CANCELED"]:
                        break
                if state == "FAILED":
                    error_message = status['QueryExecution']['Status']['StateChangeReason']
                    print(f'AthenaUtil - create_athena_raw_table - {pipeline_id} - Executing the query - Failed')
                    print(f'AthenaUtil - create_athena_raw_table - {pipeline_id} - Error - {error_message}')
                    raise Exception(f"Athena query execution failed with Error for table {pipeline_id} : {error_message}")
                else:
                    print(f'AthenaUtil - create_athena_raw_table - {pipeline_id} - Executing the query - Succeeded')
                print(f'AthenaUtil - create_athena_raw_table - {pipeline_id} - Executing the query - Finished')
            print(f'AthenaUtil - create_athena_raw_table - {pipeline_id} -in aws region - {region_name} - Finished')


    @staticmethod
    def create_athena_processed_table(pipeline_id, df, data_path, app_name, domain_name, aws_region, env):
        
        athena_us_east_1_client = boto3.client(projectConstants.ATHENA, region_name=projectConstants.US_EAST_1)
        athena_us_west_2_client  = boto3.client(projectConstants.ATHENA, region_name=projectConstants.US_WEST_2)
        athena_client_list=[athena_us_east_1_client,athena_us_west_2_client]
        col_info = df.dtypes
        s3_path = data_path[6:]
        bucket_name = s3_path.split('/')[0]

        db_name = f"datamesh_{domain_name}_processed_{env}"
        work_grp = f"datamesh-{app_name}-{env}"
        table_name = data_path.split('/')[-1]

        if len(table_name) < 1:
            table_name = data_path.split('/')[-2]
        out_loc = f"s3://{bucket_name}/logs/athena/{table_name}/"
        table_data_path = data_path.replace("s3a://", "s3://")
        context = {'Database': db_name}

        create_query_1 = f"""CREATE EXTERNAL TABLE IF NOT EXISTS `{table_name}`
             (
            """

        temp_query = """"""
        for col in col_info:
            tmp = "`{}`   {},\n".format(col[0], col[1])
            temp_query = temp_query + tmp
        create_query_2 = temp_query[:-2]

        create_query_3 = """)
        STORED AS PARQUET
        LOCATION '{}/'
        """.format(table_data_path)

        create_query = create_query_1 + create_query_2 + create_query_3
        drop_query = f"DROP TABLE IF EXISTS `{table_name}`"

        athena_queries = [drop_query, create_query]
        
        for athena_client in athena_client_list:
            region_name=athena_client.meta.region_name
            print(f'AthenaUtil - create_athena_processed_table - {pipeline_id} - in aws region - {region_name}- started')
            for query in athena_queries:
                print(f'AthenaUtil - create_athena_processed_table - {pipeline_id} - Executing the query - {query} - Started')
                res = athena_client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext=context,
                    WorkGroup=work_grp,
                    ResultConfiguration={'OutputLocation': out_loc})
                q_exec_id = res['QueryExecutionId']
                
                while True:
                    status = athena_client.get_query_execution(QueryExecutionId=q_exec_id)
                    state = status['QueryExecution']['Status']['State']
                    if state not in ["RUNNING", "QUEUED", "CANCELED"]:
                        break
                if state == "FAILED":
                    error_message = status['QueryExecution']['Status']['StateChangeReason']
                    print(f'AthenaUtil - create_athena_processed_table - {pipeline_id} - Executing the query - Failed')
                    print(f'AthenaUtil - create_athena_processed_table - {pipeline_id} - Error - {error_message}')
                    raise Exception(f"Athena query execution failed with Error : {error_message}")
                else:
                    print(f'AthenaUtil - create_athena_processed_table - {pipeline_id} - Executing the query - Succeeded')
                print(f'AthenaUtil - create_athena_processed_table - {pipeline_id} - Executing the query - Finished')
            print(f'AthenaUtil - create_athena_processed_table - {pipeline_id} - in aws region - {region_name}- Finished')
        

    @staticmethod
    def create_athena_view_using_query(pipeline_id, query, app_name, job_env):
        print(f'AthenaUtil - create_athena_view_using_query - {pipeline_id} - Executing the query - {query} - Started')
        athena_us_east_1_client = boto3.client(projectConstants.ATHENA, region_name=projectConstants.US_EAST_1)
        athena_us_west_2_client  = boto3.client(projectConstants.ATHENA, region_name=projectConstants.US_WEST_2)
    
        athena_client_list=[athena_us_east_1_client,athena_us_west_2_client]
        work_grp = "datamesh-{}-{}".format(app_name, job_env)

        for athena_client in athena_client_list:
            region_name=athena_client.meta.region_name
            print(f'AthenaUtil - create_athena_view_using_query - {pipeline_id} - in aws region - {region_name}- started')
            res = athena_client.start_query_execution(QueryString=query,WorkGroup=work_grp)
            q_exec_id = res['QueryExecutionId']
            
            while True:
                status = athena_client.get_query_execution(QueryExecutionId=q_exec_id)
                state = status['QueryExecution']['Status']['State']
                if state not in ["RUNNING", "QUEUED", "CANCELED"]:
                    break
            if state == "FAILED":
                error_message = status['QueryExecution']['Status']['StateChangeReason']
                print(f'AthenaUtil - create_athena_view_using_query - {pipeline_id} - Executing the query - Failed')
                print(f'AthenaUtil - create_athena_view_using_query - {pipeline_id} - Error - {error_message}')
                raise Exception(f"Athena query execution failed with Error for table {pipeline_id}: {error_message}")
            else:
                print(f'AthenaUtil - create_athena_view_using_query - {pipeline_id} - Executing the query - Succeeded')
        print(f'AthenaUtil - create_athena_view_using_query - {pipeline_id} - Executing the query - Finished')
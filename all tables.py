import boto3, json, base64, sys, psycopg2
from awsglue.utils import getResolvedOptions

from ingestion_framework.scripts import job_initializer
from ingestion_framework.constants import libConstants
from ingestion_framework.constants import projectConstants
from ingestion_framework.utils import postgres_util


def build_spark_options(query=None, read=False, write=False, get_uname=None):
    secret_manager_key = f"methodology/{job_environment}/rds/methodology/pgs_{job_environment}_methodology_master_liquibase-secret"
    client = boto3.client(projectConstants.SEC_MGR)
    secret_value = client.get_secret_value(SecretId=secret_manager_key)

    if projectConstants.SEC_STR in secret_value:
        secret_data = json.loads(secret_value[projectConstants.SEC_STR])
    else:
        secret_data = json.loads(base64.b64decode(secret_value[projectConstants.SEC_BIN]))

    spark_options = {}
    spark_options[projectConstants.USER] = secret_data[projectConstants.USERNAME]
    spark_options[projectConstants.PWD] = secret_data[projectConstants.PWD]
    spark_options[projectConstants.HOST] = secret_data[projectConstants.HOST]
    spark_options[projectConstants.PORT] = secret_data[projectConstants.PORT]
    spark_options[projectConstants.DBNAME] = secret_data[projectConstants.DBNAME]

    spark_options[
        "url"] = f"""jdbc:postgresql://{spark_options['host']}:{spark_options['port']}/{spark_options['dbname']}"""

    if get_uname:
        return spark_options["user"]
    if read:
        spark_options[projectConstants.FETCH_SIZE] = projectConstants.DEFAULT_SPARK_JDBC_FETCH_SIZE
        spark_options[projectConstants.QUERY] = query
    if write:
        spark_options[projectConstants.DRIVER] = libConstants.postgresDriver
    spark_options[projectConstants.SSL] = 'true'
    spark_options[projectConstants.SSL_FACTORY_PROPERTY] = projectConstants.SSL_FACTORY_VALUE
    return spark_options


def read(table, spark_options):
    try:
        read_df = spark.read.options(**spark_options).format(projectConstants.JDBC).load()
    except Exception as err:
        print(f'Failed to read the table: {table} | Error Message : {err}')
        raise SystemExit(f'Failed to read the table: {table} | Error Message : {err}')
    return read_df


def bulk_read(read_tables_list):
    read_df_dict = {}
    for table in read_tables_list:
        query = f"select * from methodology.{table}"
        spark_options = build_spark_options(query=query, read=True, write=False)
        df = read(table, spark_options)
        read_df_dict[table] = df
    return read_df_dict


# Read Source Data
# df_stg_meth_doc = read_data("stg_meth_doc")
# df_meth = read_data("meth")
# df_lkp_meth_typ = read_data("lkp_meth_typ")
# df_stg_meth_doc_grouped = df_stg_meth_doc.groupby("Methodology_Name").agg(
#     max("Methodology_Document_Version_Number")).withColumnRenamed("max(Methodology_Document_Version_Number)", "vn")
#
# df_result = df_stg_meth_doc.join(df_meth, df_stg_meth_doc.Merged_to_Methodology_name = df_meth.meth_nm, "left").join(
#     df_lkp_meth_typ, df_stg_meth_doc.Methodology_Type_Code = df_lkp_meth_typ.METH_TYPE_TXT, "inner").join(
#     df_stg_meth_doc_grouped,
#     df_stg_meth_doc_grouped.Methodology_Name = df_stg_meth_doc.Methodology_Name & df_stg_meth_doc.Methodology_Document_Version_Number = df_stg_meth_doc_grouped.vn).where(
#     df_stg_meth_doc_grouped.vn.isNotNull()).select(df_meth.meth_id, trim(df_stg_meth_doc.Methodology_Name).alias("nm"),
#                                                    df_lkp_meth_typ.meth_typ_cd,
#                                                    to_utc_timestamp(current_time())).distinct()

def build_postgres_options():
    secret_manager_key = f"methodology/{job_environment}/rds/methodology/pgs_{job_environment}_methodology_master_liquibase-secret"
    aws_client = boto3.client(projectConstants.SEC_MGR)
    secret_value = aws_client.get_secret_value(SecretId=secret_manager_key)

    if projectConstants.SEC_STR in secret_value:
        secret_data = json.loads(secret_value[projectConstants.SEC_STR])
    else:
        secret_data = json.loads(base64.b64decode(secret_value[projectConstants.SEC_BIN]))

    postgres_options = {}
    postgres_options[projectConstants.USER] = secret_data[projectConstants.USERNAME]
    postgres_options[projectConstants.PWD] = secret_data[projectConstants.PWD]
    postgres_options[projectConstants.HOST] = secret_data[projectConstants.HOST]
    postgres_options[projectConstants.DBNAME] = secret_data[projectConstants.DBNAME]
    return postgres_options


def bulk_upsert(upsert_tables_list, read_df_dict):
    postgres_options = build_postgres_options()

    for upsert_table_info in upsert_tables_list:
        # write_df = convert_source_to_target(upsert_table_info['target_table'], read_df_dict)
        target_table_name = f"methodology.{upsert_table_info['target_table']}"
        postgres_util.upsert(read_df_dict, postgres_options, target_table_name, upsert_table_info['unique_key_list'],
                             upsert_table_info['target_cols_list'], logger)
        print(target_table_name)
        read_df_dict.show()


def main():
    print(f"Execution - Started")
    table = ''
    meth_query = f"""select nextval('methodology.meth_id_seq'), a.*
from (select distinct
       c.meth_id,
       trim(Methodology_Name) nm,
       b.meth_typ_cd,
       now() at time zone 'utc',
       CURRENT_USER
from  methodology_stg.stg_meth_doc a
      left join methodology.meth c on a.Merged_to_Methodology_name = c.meth_nm
      join methodology.lkp_meth_typ b on a.Methodology_Type_Code = b.METH_TYP_desc
where Methodology_Document_Version_Number = (select max(Methodology_Document_Version_Number)
                                            from methodology_stg.stg_meth_doc b
                                            where trim(b.Methodology_Name) = trim(a.Methodology_Name)
                                            )) a"""
    spark_options = build_spark_options(query=meth_query, read=True, write=False)
    df = read(table, spark_options)

    print(f"Upsert - Started")
    meth_upsert_table = [{
        'target_table': 'meth',
        'unique_key_list': ['meth_nm'],
        'target_cols_list': ['meth_id', 'merg_to_meth_id', 'meth_nm', 'meth_typ_cd', 'modified_dtm', 'modified_user_id']
    }]
    bulk_upsert(meth_upsert_table, df)

    # meth_doc
    meth_doc_query = f"""select nextval('methodology.meth_doc_id_seq'),
	   meth_id,
	   Publishing_Entity_Code,
	   Language_Name,
	   Methodology_Document_Version_Number::int,
	   Methodology_Document_Type_Code,
	   trim(Methodology_Document_Publication_Name),
	   Methodology_Document_CMS_Document_Identifier,
	   left(Methodology_Document_RMC_Document_Identifier,-2),
       Methodology_Document_Publication_Datetime::timestamp,
	   Methodology_Document_Modification_DateTime::timestamp,
       c.meth_doc_Status_cd,
       now() at time zone 'utc',
       CURRENT_USER      
from methodology_stg.stg_meth_doc a,
     methodology.meth b,
     methodology.lkp_meth_doc_status c
where trim(a.Methodology_Name) = b.meth_nm
  and trim(a.Methodology_Status) = c.meth_doc_Status_desc
  order by meth_id,Methodology_Document_Publication_Datetime"""
    spark_options = build_spark_options(query=meth_doc_query, read=True, write=False)
    df = read(table, spark_options)

    print(f"Upsert - Started")
    meth_doc_upsert_table = [{
        'target_table': 'meth_doc',
        'unique_key_list': ['cms_doc_id'],
        'target_cols_list': ['meth_doc_id','meth_id','publ_entity_cd','lang_iso_alpha_3_cd','meth_vrsn_num',
                             'meth_doc_typ_cd','publ_titl_text','cms_doc_id','rmc_doc_id','publcn_dtm',
                             'meth_doc_modified_dtm','status_cd','modified_dtm','modified_user_id']
    }]
    bulk_upsert(meth_doc_upsert_table, df)

    # rfc_doc
    rfc_doc_query = f"""select nextval('methodology.rfc_id_seq'),
       trim(Request_for_Comment_Publication_Name),
	   Request_for_Comment_Open_Datetime::timestamp,
	   Request_for_Comment_Close_Datetime::timestamp,
	   Request_for_Comment_Original_Close_Datetime::timestamp,
       Request_for_Comment_CMS_Document_Identifier,
	   Request_for_Comment_RMC_Document_Identifier,
	   b.rfc_status_cd,
	   now() at time zone 'utc',
       CURRENT_USER	   
from methodology_stg.stg_rfc_roc a,
     methodology.lkp_rfc_status b
 where a.Request_for_Comment_Status_Code = b.rfc_status_desc"""
    spark_options = build_spark_options(query=rfc_doc_query, read=True, write=False)
    df = read(table, spark_options)

    print(f"Upsert - Started")
    rfc_doc_upsert_table = [{
        'target_table': 'rfc_doc',
        'unique_key_list': ['rfc_cms_doc_id'],
        'target_cols_list': ['rfc_id','rfc_publcn_name','rfc_open_dtm','rfc_close_dtm','rfc_origin_close_dtm',
                             'rfc_cms_doc_id','rfc_rmc_doc_id','rfc_status_cd','modified_dtm','modified_user_id']
    }]
    bulk_upsert(rfc_doc_upsert_table, df)

    # roc_doc
    roc_doc_query = f"""select rfc_id,
       Result_of_Consultation_Publication_Name,
	   Result_of_Consultation_CMS_Document_Identifier,
	   Result_of_Consultation_RMC_Document_Identifier,
	   Result_of_Consultation_Publication_Date::timestamp,
	   c.roc_status_cd,
	   now() at time zone 'utc',
       CURRENT_USER
from methodology_stg.stg_rfc_roc a,  
     methodology.rfc_doc b,
     methodology.lkp_roc_status c
where Request_for_Comment_CMS_Document_Identifier = rfc_cms_doc_id	 
  and Request_for_Comment_Publication_Name = rfc_publcn_name
  and Request_for_Comment_Open_Datetime::timestamp = rfc_open_dtm
  and Request_for_Comment_Close_Datetime::timestamp = rfc_close_dtm
  and Request_for_Comment_Original_Close_Datetime::timestamp = rfc_origin_close_dtm
  and a.Result_of_Consultation_Status_Code = c.roc_status_desc"""
    spark_options = build_spark_options(query=roc_doc_query, read=True, write=False)
    df = read(table, spark_options)

    print(f"Upsert - Started")
    roc_doc_upsert_table = [{
        'target_table': 'roc_doc',
        'unique_key_list': ['roc_cms_doc_id'],
        'target_cols_list': ['rfc_id','roc_publcn_nm','roc_cms_doc_id','roc_rmc_doc_id','roc_publcn_dtm','roc_status_cd',
                             'modified_dtm','modified_user_id']
    }]
    bulk_upsert(roc_doc_upsert_table, df)

    # meth_rfc
    meth_rfc_query = f"""select rfc_id,
       meth_doc_id,
       now() at time zone 'utc',
       CURRENT_USER
from methodology_stg.stg_rfc_roc a,
     methodology.rfc_doc b,
     methodology.meth_doc c,
     methodology_stg.stg_meth_doc d
where a.batch = d.batch
  and a.Request_for_Comment_CMS_Document_Identifier = b.rfc_cms_doc_id
  and c.lang_iso_alpha_3_cd = d.Language_Name
  and c.meth_vrsn_num = d.Methodology_Document_Version_Number::int
  and c.meth_doc_typ_cd = d.Methodology_Document_Type_Code
  and c.publ_entity_cd = d.Publishing_Entity_Code"""
    spark_options = build_spark_options(query=meth_rfc_query, read=True, write=False)
    df = read(table, spark_options)

    print(f"Upsert - Started")
    meth_rfc_upsert_table = [{
        'target_table': 'meth_rfc',
        'unique_key_list': ['rfc_id', 'meth_doc_id'],
        'target_cols_list': ['rfc_id','meth_doc_id','modified_dtm','modified_user_id']
    }]
    bulk_upsert(meth_rfc_upsert_table, df)

    print(f"Upsert - Completed")


if __name__ == '__main__':
    job_environment = getResolvedOptions(sys.argv, ['JobEnvironment'])['JobEnvironment']
    app_name = getResolvedOptions(sys.argv, ['AppName'])['AppName']
    aws_region = getResolvedOptions(sys.argv, ['AWSRegion'])['AWSRegion']

    job_name = f'datamesh-methodology-transform-{job_environment}'
    domain_name = 'reference'
    # TODO: check if the domain_name is correct
    spark = job_initializer.initializeSpark(app_name)
    logger = job_initializer.getGlueLogger(spark)
    source_target_dict = {}
    # pyspark_transformation = transformation_util.TranformationUtil(logger, spark, source_target_dict, job_name,
    #                                                                job_environment, domain_name)

    main()
import psycopg2
from psycopg2 import DatabaseError, Error
from psycopg2.extras import execute_values

from ingestion_framework.constants import projectConstants


def upsert(write_df, postgres_options, table_name_with_schema, unique_key_list, target_cols_list, logger):
    try:
        upsert_query = build_upsert_query(table_name_with_schema, unique_key_list, target_cols_list)
        print(f"build_upsert_query: {upsert_query}")
        postgres_connection = get_connection(postgres_options)
        upsert_error_messages = run_upsert(write_df, upsert_query, postgres_connection)
        print(f"Total records fetched from source - {write_df.count()}")
        upsert_error_count = len(upsert_error_messages)
        print(upsert_error_messages)
        return upsert_error_count

    except Exception as error:
        raise Exception(f"Upsert failed with error : {error}")


def build_upsert_query(table_name_with_schema, unique_key_list, target_cols_list):
    target_cols_list_str = ', '.join(target_cols_list)
    insert_query = f"INSERT INTO {table_name_with_schema} ({target_cols_list_str}) VALUES %s"

    unique_key_str = ', '.join(unique_key_list)
    update_cols_str = ', '.join(target_cols_list)

    update_cols_with_excluded_markers = [f'EXCLUDED.{col}' for col in target_cols_list]
    update_cols_with_excluded_markers_str = ', '.join(update_cols_with_excluded_markers)

    on_conflict_clause = f" ON CONFLICT ({unique_key_str}) DO UPDATE SET ({update_cols_str}) = ROW({update_cols_with_excluded_markers_str});"
    upsert_query = insert_query + on_conflict_clause
    return upsert_query


def get_connection(postgres_options):
    try:
        postgres_connection = psycopg2.connect(database=postgres_options[projectConstants.DBNAME],
                                               host=postgres_options[projectConstants.HOST],
                                               user=postgres_options[projectConstants.USER],
                                               password=postgres_options[projectConstants.PWD])
    except (Exception, DatabaseError):
        raise Exception("Unable to connect to database !!")
    return postgres_connection


def run_upsert(write_df, upsert_query, postgres_connection):
    rows_list = write_df.rdd.collect()
    batch_list = prepare_batch_list(rows_list)
    upsert_error_messages = []
    cursor = postgres_connection.cursor()

    for batch in batch_list:
        batch_error_messages = batch_upsert(cursor, batch, upsert_query)
        if batch_error_messages:
            upsert_error_messages.append(batch_error_messages)

    if len(upsert_error_messages) == 0:
        postgres_connection.commit()
    if cursor:
        cursor.close()
    if postgres_connection:
        postgres_connection.close()

    upsert_error_messages.reverse()
    upsert_error_messages = [error for batch_error_messages in upsert_error_messages for error in batch_error_messages]
    return upsert_error_messages


def prepare_batch_list(rows_list):
    batch, batch_list = [], []
    counter = 0

    for record in rows_list:
        counter += 1
        batch.append(record)
        if counter % projectConstants.UPSERT_BATCH_SIZE == 0:
            batch_list.append(batch)
            batch = []

    if batch:
        batch_list.append(batch)

    return batch_list


def batch_upsert(cursor, batch, upsert_query):
    batch_error_messages = []
    batch_list = [batch]

    while batch_list:
        batch = batch_list.pop()
        savepoint_name = 'batch_savepoint'
        try:
            cursor.execute(f"SAVEPOINT {savepoint_name};")
            execute_values(cur=cursor, sql=upsert_query, argslist=batch, page_size=len(batch))

        except (Exception, Error) as error:
            cursor.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name};")
            splitted_batches = split_batch(batch)

            if splitted_batches:
                batch_list.extend(splitted_batches)
            else:
                batch_error_messages.append(str(error))

        finally:
            cursor.execute(f"RELEASE SAVEPOINT {savepoint_name};")

    return batch_error_messages


def split_batch(batch):
    batch_size = len(batch)

    if batch_size == 1:
        return None

    chunk_size = batch_size // 2

    split_batches = [batch[i:i + chunk_size] for i in range(0, batch_size, chunk_size)]
    return split_batches


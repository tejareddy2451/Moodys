from pyspark.sql.dataframe import DataFrame
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pyspark.sql.types as T
import types
import numpy as np
import json
from datetime import datetime
from ingestion_framework.utils import logger_util, athena_util
from ingestion_framework.utils.lineage_tracker_util import insert_dynamo_record
from ingestion_framework.connector_framework.connector_utils import *


class TranformationUtil:
    def __init__(self, logger, spark, source_target_mapping, job_name, job_environment, domain_name):
        self.logger = logger
        self.spark = spark
        self.source_target_mapping = source_target_mapping
        self.job_name = job_name
        self.job_environment = job_environment
        self.domain_name = domain_name

    # Add a column of data to the table in columnData with the name columnName
    @logger_util.common_logger_exception
    def addColumn(self, data, columnName, columnData, dataType):
        """ add column of desired data type """
        print(f'Transformation Util - addColumn - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - addColumn - Adding column to data")
        if not isinstance(data, DataFrame):
            raise TypeError('data is not a dataframe')

        if not isinstance(columnName, str):
            raise TypeError('columnName is not a str')

        if not isinstance(columnData, list):
            raise TypeError('columnData is not a proper list')

        if not isinstance(columnData[0], list):
            columnData = [[i] for i in columnData]

        try:
            if isinstance(dataType, T.DataType):
                schema = data.schema
                schema = schema.add(columnName, dataType)
            elif isinstance(dataType, str):
                schemaJson = json.loads(data.schema.json())
                columnTypes = schemaJson['fields']
                columnTypes = columnTypes.append({'metadata': {}, 'name': columnName, 'type': dataType})
                schema = T.StructType.fromJson(schemaJson)

            datalist = data.rdd.collect()
            new_list = [list(datalist[i]) + columnData[i] for i in range(0, len(columnData))]
            new_data = self.spark.createDataFrame(data=new_list, schema=schema.add(columnName, dataType))
            insert_dynamo_record(self.job_name, "addColumn", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"added_col": [columnName], "final_df_cols": [str(col) for col in new_data.dtypes],
                                  "target_tables": id(new_data)})
            print(f'Transformation Util - addColumn - Finished')
            return new_data
        except Exception:
            self.logger.error('Transformation Util - addColumn - Error while adding ' + columnName)
            raise Exception

    # Add a field with a fixed value.
    @logger_util.common_logger_exception
    def addField(self, data, columnName, value, index=None):
        print(f'Transformation Util - addField - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - addField - Adding field to data")
        if not isinstance(data, DataFrame):
            raise TypeError('Transformation Util - addColumn - data is not a dataframe')

        if not isinstance(columnName, str):
            raise TypeError('Transformation Util - addColumn - columnName is not a str')

        try:
            new_data = data.withColumn(columnName, F.lit(value))
            insert_dynamo_record(self.job_name, "addField", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"added_col": [columnName], "final_df_cols": [str(col) for col in new_data.dtypes],
                                  "source_tables": [id(data)],
                                  "target_tables": id(new_data)})
            print(f'Transformation Util - addField - Finished')
            return new_data
        except Exception:
            self.logger.error('Transformation Util - addColumn - Error while adding ' + columnName)
            raise

    # Add a field with a value calculated by a lambda function.
    @logger_util.common_logger_exception
    def addComputedField(self, data, fieldName, computation, *involvedCols):
        print(f'Transformation Util - addComputedField - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - addComputedField - Adding computed field to data")

        if not isinstance(data, DataFrame):
            self.logger.error('Transformation Util - addComputedField - data passed in is not list')
            raise TypeError()

        if not isinstance(fieldName, str):
            self.logger.error('Transformation Util - addComputedField - fieldName passed in is not string')
            raise TypeError()

        if not isinstance(computation, types.LambdaType):
            self.logger.error('Transformation Util - addComputedField - computation passed in is not lambda')
            raise TypeError()

        try:
            new_data = data.withColumn(fieldName, F.udf(computation)(*involvedCols))
            insert_dynamo_record(self.job_name, "addComputedField", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"added_col": [fieldName], "final_df_cols": [str(col) for col in new_data.dtypes],
                                  "source_tables": [id(data)],
                                  "target_tables": id(new_data)})
            print(f'Transformation Util - addComputedField - Finished')
            return new_data
        except Exception:
            self.logger.error('Transformation Util - addComputedField - Error while attempting to add a computed field')
            raise

    # Add multiple fields with fixed values.
    # Columns attribute is a list and each element in that list must include a column name and value.
    @logger_util.common_logger_exception
    def addFields(self, data, columns):
        print(f'Transformation Util - addFields - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - addFields - Adding field to data")
        if not isinstance(data, DataFrame):
            raise TypeError('Transformation Util - addFields - data is not a DataFrame')

        if not isinstance(columns, list):
            raise TypeError('Transformation Util - addFields - columns is not a list')
        new_data = data
        try:
            for col in columns:
                name, value = col[0], col[1]
                new_data = new_data.withColumn(name, F.lit(value))
            insert_dynamo_record(self.job_name, "addFields", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"added_col": columns, "final_df_cols": [str(col) for col in new_data.dtypes],
                                  "source_tables": [id(data)],
                                  "target_tables": id(new_data)})
            print(f'Transformation Util - addFields - Finished')
            return new_data
        except Exception:
            self.logger.error('Transformation Util - addFields - Error while adding ' + str(columns))
            raise

    # create a new dataframe limited to chosen columns and in the order specified.
    # You can reorder existing columns or get a subset of existing columns
    @logger_util.common_logger_exception
    def getColumns(self, data, *correctOrder):
        print(f'Transformation Util - getColumns - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - getColumns - Adding field to data")

        if not isinstance(data, DataFrame):
            raise TypeError('Transformation Util - getColumns - data is not a DataFrame')

        try:
            final_data = data.select(*correctOrder)
            insert_dynamo_record(self.job_name, "getColumns", self.job_environment, self.source_target_mapping,
                                 self.domain_name, {"source_tables": [id(data)], "selected_cols": list(*correctOrder),
                                                    "final_df_cols": [str(col) for col in final_data.dtypes],
                                                    "target_tables": id(final_data)})
            print(f'Transformation Util - getColumns - Finished')
            return final_data
        except Exception:
            self.logger.error('Transformation Util - getColumns - Error in renaming columns')
            raise

    # return source dataframe with columns re-ordered so that they are in the same order as that of the target dataframe
    @logger_util.common_logger_exception
    def reorderColumnsToMatchTarget(self, source, target):
        print(
            f'Transformation Util - reorderColumnsToMatchTarget - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - reorderColumnsToMatchTarget - Reordering columns to match target data")
        insert_dynamo_record(self.job_name, "reorderColumnsToMatchTarget", self.job_environment,
                             self.source_target_mapping, self.domain_name,
                             {"source_tables": [id(source)], "selected_cols": list(*target.columns),
                              "final_df_cols": [str(col) for col in target.dtypes],
                              "target_tables": id(target)})
        print(f'Transformation Util - reorderColumnsToMatchTarget - Finished')
        return source.select(*target.columns)

    # rename columns in data according to dictionary object passed in where they key is the original name and the value is the column's new name.
    @logger_util.common_logger_exception
    def rename(self, src_data, columnsToChange):
        print(
            f'Transformation Util - rename - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - rename - Renaming columns")
        cols_before_rename = src_data.columns
        if not isinstance(src_data, DataFrame):
            raise TypeError('Transformation Util - rename - data is not a list')

        if not isinstance(columnsToChange, dict):
            raise TypeError('Transformation Util - rename - columnsToChange is not a dictionary')

        try:
            data = src_data
            for col, new_col in columnsToChange.items():
                data = data.withColumnRenamed(col, new_col)
            insert_dynamo_record(self.job_name, "rename", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"selected_cols": cols_before_rename, "renamed_cols": columnsToChange,
                                  "source_tables": [id(src_data)],
                                  "final_df_cols": [str(col) for col in data.dtypes], "target_tables": id(data)})
            print(
                f'Transformation Util - rename - Finished')
            return data
        except Exception:
            self.logger.error('Transformation Util - rename - Error in renaming columns')
            raise

    # drop columns in df according to how many rows are null.
    # 'any' : If any NA values are present, drop that column.
    # 'all' : If all values are NA, drop that column.
    @logger_util.common_logger_exception
    def dropnacolumns(self, df, how='any'):
        print(f'Transformation Util - dropnacolumns - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - dropnacolumns - dropping columns with " + how + " null rows")
        columnsToDrop = []
        numRows = df.count()
        cols_before_drop = df.columns
        if how.lower() == 'all':
            for col in df.columns:
                withoutnulls = df.select(col).dropna()
                if withoutnulls.count() == 0:  # all values in column were null
                    columnsToDrop.append(col)
        elif how.lower() == 'any':
            for col in df.columns:
                withoutnulls = df.select(col).dropna()
                if withoutnulls.count() < numRows:
                    columnsToDrop.append(col)
        else:
            raise ("Transformation Util - dropnacolumns - invalid input for how")
        try:
            data = df.drop(*columnsToDrop)
            insert_dynamo_record(self.job_name, "dropnacolumns", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"source_tables": [id(df)], "selected_cols": cols_before_drop,
                                  "dropped_cols": columnsToDrop, "final_df_cols": [str(col) for col in data.dtypes],
                                  "target_tables": id(data)})
            print("Transformation Util - dropnacolumns - Finished")
            return data
        except Exception:
            self.logger.error("Transformation Util - dropnacolumns - couldn't drop columns")
            raise Exception

    # convert pandas DataFrame to pyspark DataFrame while retaining schema and column names.
    # Important: this conversion function will convert every column's data type to string
    # in the pyspark dataframe.
    # To retain your pandas Dataframe's data types either infer or create your desired schema explicitly.
    @logger_util.common_logger_exception
    def pandasToSparkDF(self, pandas_df):
        print(f'Transformation Util - pandasToSparkDF - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - pandasToSparkDF - creating pyspark dataframe from pandas dataframe")
        schema = [T.StructField(col, T.StringType(), True) for col in pandas_df.columns.to_list()]
        if pandas_df.empty:
            return self.spark.createDataFrame([], T.StructType(schema))
        else:
            return self.spark.createDataFrame(pandas_df, T.StructType(schema)).replace('NaN', None)

    # returns True if dataframes passed in are equal and prints a message to log file
    # indicating what is currently happening in the process.
    @logger_util.common_logger_exception
    def testDataFramesEqual(self, actual, expected, message):
        print(f'Transformation Util - testDataFramesEqual - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - testDataFramesEqual - test equality of two dataframes")
        try:
            actual_count = actual.count()
            expected_count = expected.count()
            if set(actual.columns) == set(expected.columns):
                if actual_count >= expected_count:
                    diff = actual.select(expected.columns).subtract(expected.select(expected.columns))
                else:
                    diff = expected.select(expected.columns).subtract(actual.select(expected.columns))
                if diff.count() == 0:
                    print(message)
                    return True
                else:
                    if set(actual.collect()) == set(expected.collect()):
                        print(message)
                        return True
                    else:
                        print("difference in rows")
                        return False
            else:
                print("actual columns: " + str(actual.columns))
                print("expected columns: " + str(expected.columns))
                return False
        except Exception:
            actual.printSchema()
            expected.printSchema()
            raise Exception

    # Perform an equi-join/inner join on the given tables.
    # Pass column keys in as list to keyColumns (if column names are the same)
    # or leftKeys and rightKeys (if column names are different).
    @logger_util.common_logger_exception
    def join(self, leftSideData, rightSideData, keyColumns, leftKeys=None, rightKeys=None, leftPrefix=None,
             rightPrefix=None):
        print(f'Transformation Util - join - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - join - join two dataframes")

        if not isinstance(leftSideData, DataFrame):
            raise TypeError('Transformation Util - join - leftSideData passed in is not list')

        if not isinstance(rightSideData, DataFrame):
            raise TypeError('Transformation Util - join - rightSideData passed in is not list')

        if not isinstance(keyColumns, list) and not isinstance(keyColumns, str):
            raise TypeError('Transformation Util - join - keyColumns passed in is not list')

        try:
            if isinstance(leftKeys, list) and isinstance(rightKeys, list) and len(leftKeys) == len(rightKeys):
                rightcol = rightSideData.columns
                for col in rightKeys:
                    rightcol.remove(col)
                correctorder = leftSideData.columns + rightcol

                joincondition = []
                for idx in range(0, len(leftKeys)):
                    joincondition.append(leftSideData[leftKeys[idx]] == rightSideData[rightKeys[idx]])

                data = leftSideData.join(rightSideData, joincondition).select(correctorder)
            elif isinstance(leftKeys, str) and isinstance(rightKeys, str):
                correctorder = rightSideData.columns.remove(rightKeys)

                data = leftSideData.join(rightSideData, leftSideData[leftKeys] == rightSideData[rightKeys]).select(
                    correctorder)
            elif isinstance(keyColumns, list):
                rightcol = rightSideData.columns
                for col in keyColumns:
                    rightcol.remove(col)

                correctorder = leftSideData.columns + rightcol
                data = leftSideData.join(rightSideData, keyColumns).select(correctorder)
            elif isinstance(keyColumns, str):
                rightcol = rightSideData.columns
                rightcol.remove(keyColumns)
                correctorder = leftSideData + rightcol
                data = leftSideData.join(rightSideData, keyColumns).select(correctorder)
            else:
                self.logger.error("Transformation Util - join - keyColumns not a recognized data type")
                raise TypeError("Transformation Util - join - keyColumns not a recognized data type")
            insert_dynamo_record(self.job_name, "join", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"left_df_cols": [str(col) for col in leftSideData.dtypes],
                                  "right_df_cols": [str(col) for col in rightSideData.dtypes],
                                  "left_key_cols": leftKeys if isinstance(leftKeys, list) else [leftKeys],
                                  "right_key_cols": rightKeys if isinstance(rightKeys, list) else [
                                      rightKeys],
                                  "final_df_cols": [str(col) for col in data.dtypes], "target_tables": id(data),
                                  "source_tables": [id(leftSideData), id(rightSideData)],
                                  "left_df_id": str(id(leftSideData)), "right_df_id": str(id(rightSideData))})
            print("Transformation Util - join - Finished")
            return data
        except Exception:
            self.logger.error('Transformation Util - join - Error while attempting to join datasets')
            raise

    # Perform a left outer join on the given tables.
    # Pass column keys in as list to keyColumns (if column names are the same)
    @logger_util.common_logger_exception
    def leftJoin(self, leftSideData, rightSideData, keyColumns, suffixes=None):
        print(f'Transformation Util - leftJoin - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - leftJoin - left join two dataframes")

        if not isinstance(leftSideData, DataFrame):
            raise TypeError('Transformation Util - leftJoin - leftSideData passed in is not list')

        if not isinstance(rightSideData, DataFrame):
            raise TypeError('Transformation Util - leftJoin - rightSideData passed in is not list')

        if not isinstance(keyColumns, list) and not isinstance(keyColumns, str):
            raise TypeError('Transformation Util - leftJoin - keyColumns passed in is not list')

        if suffixes is not None and not isinstance(suffixes, list):
            raise TypeError('Transformation Util - leftJoin - suffixes passed in is not list')

        try:
            if isinstance(keyColumns, list):
                rightcol = rightSideData.columns
                for col in keyColumns:
                    rightcol.remove(col)
            elif isinstance(keyColumns, str):
                rightcol = rightSideData.columns
                rightcol.remove(keyColumns)
            else:
                raise TypeError("Transformation Util - leftJoin - keyColumns not a recognized data type")
            correctorder = leftSideData.columns + rightcol
            resultData = leftSideData.join(rightSideData, keyColumns, 'left').select(correctorder)
            insert_dynamo_record(self.job_name, "leftJoin", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"left_df_cols": [str(col) for col in leftSideData.dtypes],
                                  "right_df_cols": [str(col) for col in rightSideData.dtypes],
                                  "key_cols": keyColumns if isinstance(keyColumns, list) else [keyColumns],
                                  "final_df_cols": [str(col) for col in resultData.dtypes],
                                  "target_tables": id(resultData),
                                  "source_tables": [id(leftSideData), id(rightSideData)],
                                  "left_df_id": str(id(leftSideData)), "right_df_id": str(id(rightSideData))})
            print("Transformation Util - leftJoin - Finished")
            return resultData
        except Exception:
            self.logger.error('Transformation Util - leftJoin - Error while attempting to leftJoin datasets')
            raise

    # Perform an inner join on the given tables. Pass column keys in as list to
    # keyColumns if column names are the same.
    @logger_util.common_logger_exception
    def innerJoin(self, leftSideData, rightSideData, keyColumns, suffixes=None):
        print(f'Transformation Util - innerJoin - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - innerJoin - inner join two dataframes")

        if not isinstance(leftSideData, DataFrame):
            raise TypeError('Transformation Util - innerJoin - leftSideData passed in is not list')

        if not isinstance(rightSideData, DataFrame):
            raise TypeError('Transformation Util - innerJoin - rightSideData passed in is not list')

        if not isinstance(keyColumns, list) and not isinstance(keyColumns, str):
            raise TypeError('Transformation Util - innerJoin - keyColumns passed in is not list')

        if suffixes is not None and not isinstance(suffixes, list):
            raise TypeError('Transformation Util - innerJoin - suffixes passed in is not list')

        try:
            if isinstance(keyColumns, list):
                rightcol = rightSideData.columns
                for col in keyColumns:
                    rightcol.remove(col)
            elif isinstance(keyColumns, str):
                rightcol = rightSideData.columns
                rightcol.remove(keyColumns)
            else:
                raise TypeError("Transformation Util - innerJoin - keyColumns not a recognized data type")
            correctorder = leftSideData.columns + rightcol
            resultData = leftSideData.join(rightSideData, keyColumns, 'inner').select(correctorder)
            insert_dynamo_record(self.job_name, "innerJoin", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"left_df_cols": [str(col) for col in leftSideData.dtypes],
                                  "right_df_cols": [str(col) for col in rightSideData.dtypes],
                                  "key_cols": keyColumns if isinstance(keyColumns, list) else [keyColumns],
                                  "final_df_cols": [str(col) for col in resultData.dtypes],
                                  "target_tables": id(resultData),
                                  "source_tables": [id(leftSideData), id(rightSideData)],
                                  "left_df_id": str(id(leftSideData)), "right_df_id": str(id(rightSideData))})
            print("Transformation Util - innerJoin - Finished")
            return resultData
        except Exception:
            self.logger.error('Transformation Util - innerJoin - Error while attempting to leftJoin datasets')
            raise

    # Perform a full outer join on the given tables.
    # Pass column keys in as list to keyColumns (if column names are the same).
    @logger_util.common_logger_exception
    def outerJoin(self, leftSideData, rightSideData, keyColumns, suffixes):
        print(f'Transformation Util - outerJoin - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - outerJoin - outer join two dataframes")

        if not isinstance(leftSideData, DataFrame):
            raise TypeError('Transformation Util - outerJoin - leftSideData passed in is not list')

        if not isinstance(rightSideData, DataFrame):
            raise TypeError('Transformation Util - outerJoin - rightSideData passed in is not list')

        if not isinstance(keyColumns, list) and not isinstance(keyColumns, str):
            raise TypeError('Transformation Util - outerJoin - keyColumns passed in is not list')

        if not isinstance(suffixes, list):
            raise TypeError('Transformation Util - outerJoin - suffixes passed in is not list')

        try:
            if isinstance(keyColumns, list):
                rightcol = rightSideData.columns
                for col in keyColumns:
                    rightcol.remove(col)
            elif isinstance(keyColumns, str):
                rightcol = rightSideData.columns
                rightcol.remove(keyColumns)
            else:
                raise TypeError("Transformation Util - outerJoin - keyColumns not a recognized data type")
            correctorder = leftSideData.columns + rightcol
            resultData = leftSideData.join(rightSideData, keyColumns, 'outer').select(correctorder)
            insert_dynamo_record(self.job_name, "outerJoin", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"left_df_cols": [str(col) for col in leftSideData.dtypes],
                                  "right_df_cols": [str(col) for col in rightSideData.dtypes],
                                  "key_cols": keyColumns if isinstance(keyColumns, list) else [keyColumns],
                                  "final_df_cols": [str(col) for col in resultData.dtypes],
                                  "target_tables": id(resultData),
                                  "left_df_id": str(id(leftSideData)), "right_df_id": str(id(rightSideData))})
            print("Transformation Util - outerJoin - Finished")
            return resultData
        except Exception:
            self.logger.error('Transformation Util - outerJoin - Error while attempting to outerJoin datasets')
            raise

    # Perform a left join, but where the key is not unique in the right-hand table,
    # arbitrarily choose the first row and ignore others.
    @logger_util.common_logger_exception
    def lkpJoin(self, leftSideData, rightSideData, keyColumns=None, leftKeys=None, rightKeys=None, leftPrefix=None,
                rightPrefix=None):
        print(f'Transformation Util - lkpJoin - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - lkpJoin - lkp join two dataframes")

        if not isinstance(leftSideData, DataFrame):
            raise TypeError('Transformation Util - lkpJoin - leftSideData passed in is not list')

        if not isinstance(rightSideData, DataFrame):
            raise TypeError('Transformation Util - lkpJoin - rightSideData passed in is not list')

        try:
            window = Window.orderBy(keyColumns).partitionBy(keyColumns)
            rightSideData = rightSideData.withColumn("row", F.row_number().over(window)).filter(F.col("row") == 1).drop(
                "row")

            if isinstance(leftKeys, list) and isinstance(rightKeys, list):
                joincondition = []
                for idx in range(0, len(leftKeys)):
                    joincondition.append(leftSideData[leftKeys[idx]] == rightSideData[rightKeys[idx]])

            elif isinstance(leftKeys, str) and isinstance(rightKeys, str):
                joincondition = (leftSideData[leftKeys[idx]] == rightSideData[rightKeys[idx]])

            elif leftKeys == None and rightKeys == None and isinstance(keyColumns, list):
                joincondition = []
                for idx in keyColumns:
                    joincondition.append(leftSideData[idx] == rightSideData[idx])

            elif isinstance(keyColumns, str):
                joincondition = (leftSideData[keyColumns] == rightSideData[keyColumns])

            rightcol = rightSideData.columns
            for col in keyColumns:
                rightcol.remove(col)
            correctorder = leftSideData.columns + rightcol
            data = leftSideData.join(rightSideData, joincondition, 'left').select(correctorder)
            insert_dynamo_record(self.job_name, "lkpJoin", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"left_df_cols": [str(col) for col in leftSideData.dtypes],
                                  "right_df_cols": [str(col) for col in rightSideData.dtypes],
                                  "key_cols": keyColumns if isinstance(keyColumns, list) else [keyColumns],
                                  "final_df_cols": [str(col) for col in data.dtypes], "target_tables": id(data),
                                  "left_df_id": str(id(leftSideData)), "right_df_id": str(id(rightSideData))})
            print("Transformation Util - lkpJoin - Finished")
            return data
        except Exception:
            self.logger.error('Transformation Util - lkpJoin - Error while attempting to lookupjoin datasets')
            raise

    # Return rows from the left table where the key value does not occur in the
    # right table.
    @logger_util.common_logger_exception
    def antiJoin(self, leftSideData, rightSideData, keyColumns):
        print(f'Transformation Util - antiJoin - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - antiJoin - anti join two dataframes")

        if not isinstance(leftSideData, DataFrame):
            raise TypeError('Transformation Util - antiJoin - leftSideData is not a Dataframe')

        if not isinstance(rightSideData, DataFrame):
            raise TypeError('Transformation Util - antiJoin - rightSideData is not a Dataframe')

        if not isinstance(keyColumns, list):
            raise TypeError('Transformation Util - antiJoin - keyColumns is not a list')

        try:
            antiJoinData = leftSideData.join(rightSideData, keyColumns[0], "leftanti").select(leftSideData.columns)
            insert_dynamo_record(self.job_name, "antiJoin", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"left_df_cols": [str(col) for col in leftSideData.dtypes],
                                  "right_df_cols": [str(col) for col in rightSideData.dtypes],
                                  "key_cols": keyColumns if isinstance(keyColumns, list) else [keyColumns],
                                  "final_df_cols": [str(col) for col in antiJoinData.dtypes],
                                  "target_tables": id(antiJoinData),
                                  "left_df_id": str(id(leftSideData)), "right_df_id": str(id(rightSideData))})
            print("Transformation Util - antiJoin - Finished")
            return antiJoinData
        except Exception:
            self.logger.error('Transformation Util - antiJoin - Error while performing antiJoin')
            raise

    # Perform left join on dataframes that do not have a column name in common
    @logger_util.common_logger_exception
    def leftJoinDifferentKeyNames(self, leftSideData, rightSideData, leftKeyColumns, rightKeyColumns, suffixes=None):
        print(
            f'Transformation Util - leftJoinDifferentKeyNames - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - leftJoinDifferentKeyNames - left join two dataframes with different key names")

        if not isinstance(leftSideData, DataFrame):
            raise TypeError('Transformation Util - leftJoinDifferentKeyNames - leftSideData passed in is not list')

        if not isinstance(rightSideData, DataFrame):
            raise TypeError('Transformation Util - leftJoinDifferentKeyNames - rightSideData passed in is not list')

        if not isinstance(leftKeyColumns, list):
            raise TypeError('Transformation Util - leftJoinDifferentKeyNames - leftKeyColumns passed in is not list')

        if not isinstance(rightKeyColumns, list):
            raise TypeError('Transformation Util - leftJoinDifferentKeyNames - rightKeyColumns passed in is not list')

        if suffixes is not None and not isinstance(suffixes, DataFrame):
            raise TypeError('Transformation Util - leftJoinDifferentKeyNames - suffixes passed in is not list')

        try:
            rightcol = rightSideData.columns
            correctorder = leftSideData.columns + rightcol

            joincondition = []
            for idx in range(0, len(leftKeyColumns)):
                joincondition.append(leftSideData[leftKeyColumns[idx]] == rightSideData[rightKeyColumns[idx]])

            resultData = leftSideData.join(rightSideData, joincondition, 'left').select(correctorder)
            insert_dynamo_record(self.job_name, "leftJoinDifferentKeyNames", self.job_environment,
                                 self.source_target_mapping, self.domain_name,
                                 {"left_df_cols": [str(col) for col in leftSideData.dtypes],
                                  "right_df_cols": [str(col) for col in rightSideData.dtypes],
                                  "left_key_cols": leftKeyColumns if isinstance(leftKeyColumns, list) else [
                                      leftKeyColumns],
                                  "right_key_cols": rightKeyColumns if isinstance(rightKeyColumns, list) else [
                                      rightKeyColumns],
                                  "final_df_cols": [str(col) for col in resultData.dtypes],
                                  "target_tables": id(resultData),
                                  "source_tables": [id(leftSideData), id(rightSideData)],
                                  "left_df_id": str(id(leftSideData)), "right_df_id": str(id(rightSideData))})
            print("Transformation Util - leftJoinDifferentKeyNames - Finished")
            return resultData
        except Exception:
            self.logger.error(
                'Transformation Util - leftJoinDifferentKeyNames - Error while attempting to leftJoin datasets')
            raise

    # Return rows in leftSideData that are not in rightSideData.
    @logger_util.common_logger_exception
    def complement(self, leftSideData, rightSideData):
        print(
            f'Transformation Util - complement - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - complement - complement two dataframes")
        print("complement")

        if not isinstance(leftSideData, DataFrame):
            raise TypeError('Transformation Util - complement - leftSideData is not a list')

        if not isinstance(rightSideData, DataFrame):
            raise TypeError('Transformation Util - complement - rightSideData is not a list')
        try:
            complementData = leftSideData.subtract(rightSideData.select(leftSideData.schema.names))
            insert_dynamo_record(self.job_name, "complement", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"left_df_cols": [str(col) for col in leftSideData.dtypes],
                                  "right_df_cols": [str(col) for col in rightSideData.dtypes],
                                  "final_df_cols": [str(col) for col in complementData.dtypes],
                                  "target_tables": id(complementData),
                                  "source_tables": [id(leftSideData), id(rightSideData)],
                                  "left_df_id": str(id(leftSideData)), "right_df_id": str(id(rightSideData))})
            print("Transformation Util - complement - Finished")
            return complementData
        except Exception:
            self.logger.error('Transformation Util - complement - Error while performing complement')
            raise

    # Remove specified fields from dataframe (drop).
    @logger_util.common_logger_exception
    def cutOut(self, data, columnNames):
        print(f'Transformation Util - cutOut - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - cutOut - drop columns in dataframe")
        cols_before_drop = [str(col) for col in data.dtypes]
        if not isinstance(data, DataFrame):
            raise TypeError('Transformation Util - cutOut - data passed in is not list')

        try:
            new_data = data.drop(*columnNames)
            insert_dynamo_record(self.job_name, "cutOut", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"selected_cols": cols_before_drop, "dropped_cols": columnNames,
                                  "source_tables": [id(data)],
                                  "final_df_cols": [str(col) for col in new_data.dtypes],
                                  "target_tables": id(new_data)})
            print("Transformation Util - cutOut - Finished")
            return new_data
        except Exception:
            self.logger.error('Transformation Util - cutOut - Error while attempting to cutout columns from data set')
            raise

    # Select rows from the given dataframe that evaluate select condition.
    @logger_util.common_logger_exception
    def selectRows(self, data, select, *involvedCols):
        print(f'Transformation Util - selectRows - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - selectRows - filter rows in dataframe")

        if not isinstance(data, DataFrame):
            self.logger.error('Transformation Util - selectRows - data passed in is not list')
            raise TypeError()

        if not isinstance(select, T.BooleanType) and not isinstance(select, str):
            self.logger.error('Transformation Util - selectRows - select passed in is not lambda or string')
            raise TypeError()

        if isinstance(select, types.LambdaType) and involvedCols is None:
            self.logger.error("Transformation Util - selectRows - did not select columns to apply condition")
            raise TypeError()

        try:
            if isinstance(select, str):
                new_data = data.filter(select)
            elif isinstance(select, types.LambdaType):
                new_data = data.addComputedField(data, "_condition", select, *involvedCols).filter(
                    "_condition == True").drop("_condition")
            insert_dynamo_record(self.job_name, "selectRows", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"selected_cols": [str(col) for col in data.dtypes],
                                  "source_tables": [id(data)],
                                  "final_df_cols": [str(col) for col in new_data.dtypes],
                                  "target_tables": id(new_data)})
            print("Transformation Util - selectRows - Finished")
            return new_data
        except Exception:
            self.logger.error('Transformation Util - selectRows - Error while attempting to select rows')
            raise

    # Convert the DataFrame to a dictionary by passing in a key and a value column.
    @logger_util.common_logger_exception
    def dfToDict(self, data, keyColumn, valueColumn, dict={}):
        print(f'Transformation Util - dfToDict - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - dfToDict - get dictionary from dataframe columns")

        if not isinstance(data, DataFrame):
            self.logger.error('Transformation Util - dfToDict - data passed in is not list')
            raise TypeError()

        if not isinstance(keyColumn, str) or not isinstance(valueColumn, str):
            self.logger.error('Transformation Util - dfToDict - column name passed in is not string')

        try:
            for row in data.select(keyColumn, valueColumn).collect():
                dict[row[keyColumn]] = row[valueColumn]

            return dict
        except Exception:
            self.logger.error('Transformation Util - dfToDict - Error in assigning dict')
            raise Exception

    # Convert dataframe column to list object
    @logger_util.common_logger_exception
    def columnToListRemoveDuplicates(self, data, colName):
        print(
            f'Transformation Util - columnToListRemoveDuplicates - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print("Transformation Util - columnToListRemoveDuplicates - Convert dataframe column to list object")

        if not isinstance(data, DataFrame):
            self.logger.error('Transformation Util - columnToListRemoveDuplicates - data passed in is not list')
            raise TypeError()

        if not isinstance(colName, str):
            self.logger.error(
                'Transformation Util - columnToListRemoveDuplicates - column name passed in is not string')

        try:
            return [row[0] for row in data.select(colName).distinct().collect()]
        except Exception:
            self.logger.error('Transformation Util - columnToListRemoveDuplicates - Error in converting column to list')
            raise Exception

    # This function will create a surrogate key by connecting to a database and pulling its data into a dataframe.
    @logger_util.common_logger_exception
    def getLkpSurrogateKeyValue(self, lkpTableName, schemaName, lkpNaturalKeyColumns, lkpSurrogateKeyColumn,
                                targetForeignKeyColumn, sourceDataJoinColumns,
                                sourceData, secret_name, source_type, source_database_type, env_type, joinType):
        print(
            f'Transformation Util - getLkpSurrogateKeyValue - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - getLkpSurrogateKeyValue - create a surrogate key by connecting to a database and pulling its data into a dataframe")

        if not isinstance(schemaName, str):
            raise TypeError('Transformation Util - getLkpSurrogateKeyValue - schemaName passed in is not string')

        if not isinstance(lkpTableName, str):
            raise TypeError('Transformation Util - getLkpSurrogateKeyValue - lkpTableName passed in is not string')

        if not isinstance(lkpSurrogateKeyColumn, str):
            raise TypeError(
                'Transformation Util - getLkpSurrogateKeyValue - lkpSurrogateKeyColumn passed in is not string')

        if not isinstance(joinType, str):
            raise TypeError('Transformation Util - getLkpSurrogateKeyValue - joinType passed in is not string')

        if not isinstance(targetForeignKeyColumn, str):
            raise TypeError(
                'Transformation Util - getLkpSurrogateKeyValue - targetForeignKeyColumn passed in is not string')

        if not isinstance(lkpNaturalKeyColumns, list):
            raise TypeError(
                'Transformation Util - getLkpSurrogateKeyValue - lkpNaturalKeyColumns passed in is not list')

        if not isinstance(sourceDataJoinColumns, list):
            raise TypeError(
                'Transformation Util - getLkpSurrogateKeyValue - sourceDataJoinColumns passed in is not list')

        if not isinstance(sourceData, DataFrame):
            raise TypeError('Transformation Util - getLkpSurrogateKeyValue - sourceData passed in is not a list')

        if len(sourceDataJoinColumns) != len(lkpNaturalKeyColumns):
            raise TypeError(
                'Transformation Util - getLkpSurrogateKeyValue - Natural key columns on lookup table are not equal to join columns in source data')

        try:
            selectColumns = lkpNaturalKeyColumns + [lkpSurrogateKeyColumn]
            lkpExtractQuery = 'select ' + ','.join(selectColumns) + ' from ' + schemaName + '.' + lkpTableName
            config_dict = {
                "source_type": source_type,
                "source_database_type": source_database_type,
                "source_environments": '[{"environment_name": "{}", "database_secret_manager_name": "{}"}]'.format(
                    env_type, secret_name),
                "source_query": lkpExtractQuery
            }
            src_dict = InventoryReader(config_dict).get_source_connector_config()
            src_connector = ConnectorSupplier(env_type).get_connector(src_dict)
            lkpData = src_connector.read_data()
            columnDict = dict(zip(lkpNaturalKeyColumns, sourceDataJoinColumns))
            renamedLkpData = rename(lkpData, columnDict)
            sourceHeader = sourceData.schema.names
            dataDf = sourceData
            lkpDf = renamedLkpData
            sourceData = dataDf.join(lkpDf, on=sourceDataJoinColumns, how=joinType)
            sourceHeader = sourceHeader.append(lkpSurrogateKeyColumn)
            for key in sourceDataJoinColumns:
                sourceData = cutOut(sourceData, key)

            return rename(sourceData, {lkpSurrogateKeyColumn: targetForeignKeyColumn})
        except Exception:
            self.logger.error('Transformation Util - getLkpSurrogateKeyValue - Error retrieving surrogate key')
            raise

    # this function adds a surrogate key from a
    @logger_util.common_logger_exception
    def getLkpSurrogateKeyValue(self, lkpData, lkpNaturalKeyColumns, lkpSurrogateKeyColumn, targetForeignKeyColumn,
                                sourceDataJoinColumns,
                                sourceData, joinType):
        print(
            f'Transformation Util - getLkpSurrogateKeyValue - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - getLkpSurrogateKeyValue - adds a surrogate key")

        if not isinstance(lkpSurrogateKeyColumn, str):
            raise TypeError(
                'Transformation Util - getLkpSurrogateKeyValue - lkpSurrogateKeyColumn passed in is not string')

        if not isinstance(joinType, str):
            raise TypeError('Transformation Util - getLkpSurrogateKeyValue - joinType passed in is not string')

        if not isinstance(targetForeignKeyColumn, str):
            raise TypeError(
                'Transformation Util - getLkpSurrogateKeyValue - targetForeignKeyColumn passed in is not string')

        if not isinstance(lkpNaturalKeyColumns, list):
            raise TypeError(
                'Transformation Util - getLkpSurrogateKeyValue - lkpNaturalKeyColumns passed in is not list')

        if not isinstance(sourceDataJoinColumns, list):
            raise TypeError(
                'Transformation Util - getLkpSurrogateKeyValue - sourceDataJoinColumns passed in is not list')

        if not isinstance(sourceData, DataFrame):
            raise TypeError('Transformation Util - getLkpSurrogateKeyValue - sourceData passed in is not a list')

        if len(sourceDataJoinColumns) != len(lkpNaturalKeyColumns):
            raise TypeError(
                'Transformation Util - getLkpSurrogateKeyValue - Natural key columns on lookup table are not equal to join columns in source data')

        try:
            selectColumns = lkpNaturalKeyColumns + [lkpSurrogateKeyColumn]
            lkpData = lkpData.select(selectColumns)
            columnDict = dict(zip(lkpNaturalKeyColumns, sourceDataJoinColumns))
            renamedLkpData = rename(lkpData, columnDict)
            sourceHeader = sourceData.schema.names
            dataDf = sourceData
            lkpDf = renamedLkpData
            sourceData = dataDf.join(lkpDf, on=sourceDataJoinColumns, how=joinType)
            sourceHeader = sourceHeader.append(lkpSurrogateKeyColumn)
            for key in sourceDataJoinColumns:
                sourceData = cutOut(sourceData, key)

            return rename(sourceData, {lkpSurrogateKeyColumn: targetForeignKeyColumn})
        except Exception:
            self.logger.error('Transformation Util - getLkpSurrogateKeyValue - Error retrieving surrogate key')
            raise

    # helper method for findFirstNonNullColumn
    @logger_util.common_logger_exception
    def findFirstNonNull(self, *args):
        n = 0
        for a in args:
            if a in ['NR', 'WR', 'WD', 'PIF', 'nan', np.nan, None, '', 'N/A']:
                n += 1
                continue
            else:
                return a[n]
        return None

    # for every row, find the first non-null value and return that value as a new column.
    @logger_util.common_logger_exception
    def findFirstNonNullColumn(self, data, colName, *cols):
        print("Transformation Util - findFirstNonNullColumn - find first value in row that is not null")
        return addComputedField(data, colName, lambda *x: findFirstNonNull(x), cols)

    # Transform values in one field via a lambda function.
    @logger_util.common_logger_exception
    def conversion(self, data, columnName, conversion, dataType):
        print(
            f'Transformation Util - conversion - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - conversion - Transform values in one field via a lambda function")

        if not isinstance(data, DataFrame):
            raise TypeError('Transformation Util - conversion - data passed in is not dataframe')

        if not isinstance(columnName, str):
            raise TypeError('Transformation Util - conversion - columnName passed in is not string')

        if not isinstance(conversion, types.LambdaType):
            raise TypeError('Transformation Util - conversion - conversion passed in is not lambda')

        if not isinstance(dataType, T.DataType):
            raise TypeError(
                'Transformation Util - conversion - datatype is not an instance of pyspark.sql.types.DataType')

        try:
            if isinstance(columnName, str) and columnName in data.columns:
                new_data = data.withColumn(columnName, F.udf(conversion, dataType)(columnName))
            elif isinstance(columnName, list) or isinstance(columnName,
                                                            tuple):  ##check if more than one column name passed in
                new_data = data
                for col in columnName:
                    if col in data.columns:
                        new_data = new_data.withColumn(col, F.udf(conversion, dataType)(col))
                    else:
                        raise KeyError("Transformation Util - conversion - " + col + " is not a valid column name")
            else:
                raise KeyError("Transformation Util - conversion - error in input")
            insert_dynamo_record(self.job_name, "conversion", self.job_environment,
                                 self.source_target_mapping, self.domain_name,
                                 {"selected_cols": [str(col) for col in data.dtypes],
                                  "source_tables": [id(data)],
                                  "final_df_cols": [str(col) for col in new_data.dtypes],
                                  "target_tables": id(new_data)})
            print("Transformation Util - conversion - Finished")
            return data
        except Exception:
            self.logger.error('Transformation Util - conversion - Error while attempting a conversion')
            raise

    # apply conversion only to rows where condition is True
    @logger_util.common_logger_exception
    def conditionalConversion(self, data, columnName, conversion, condition):
        print(
            f'Transformation Util - conditionalConversion - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - conditionalConversion - apply conversion only to rows where condition is True")

        if not isinstance(data, DataFrame):
            raise TypeError('Transformation Util - conditionalConversion - data passed in is not list')

        if not isinstance(columnName, str):
            raise TypeError('Transformation Util - conditionalConversion - columnName passed in is not string')

        if not isinstance(conversion, types.LambdaType):
            raise TypeError('Transformation Util - conditionalConversion - conversion passed in is not lambda')

        if not isinstance(condition, T.BooleanType) and not isinstance(condition, str):
            raise TypeError('Transformation Util - conditionalConversion - condition passed in is not boolean')

        try:
            changedRows = data.filter(condition)
            sameRows = data.subtract(changedRows)
            new_data = changedRows.withColumn(columnName, F.udf(conversion)(columnName)).union(sameRows)
            insert_dynamo_record(self.job_name, "conditionalConversion", self.job_environment,
                                 self.source_target_mapping, self.domain_name,
                                 {"selected_cols": [str(col) for col in data.dtypes],
                                  "source_tables": [id(data)],
                                  "final_df_cols": [str(col) for col in new_data.dtypes],
                                  "target_tables": id(new_data)})
            print("Transformation Util - conditionalConversion - Finished")
            return new_data
        except Exception:
            self.logger.error(
                'Transformation Util - conditionalConversion - Error while attempting a conditional conversion')
            raise

    # converts column to pyspark.sql.types.FloatType
    @logger_util.common_logger_exception
    def convertColumnToFloat(self, data, columnName):
        print(
            f'Transformation Util - convertColumnToFloat - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - convertColumnToFloat - converts column to pyspark.sql.types.FloatType")

        try:
            convertedData = data.withColumn(columnName, data[columnName].cast("float"))
            insert_dynamo_record(self.job_name, "convertColumnToFloat", self.job_environment,
                                 self.source_target_mapping, self.domain_name,
                                 {"selected_cols": [str(col) for col in data.dtypes],
                                  "source_tables": [id(data)],
                                  "final_df_cols": [str(col) for col in convertedData.dtypes],
                                  "target_tables": id(convertedData)})
            print("Transformation Util - convertColumnToFloat - Finished")
            return convertedData
        except Exception:
            self.logger.error('Transformation Util - convertColumnToFloat - Error while converting column into float')
            raise

    # converts column to pyspark.sql.types.IntegerType
    @logger_util.common_logger_exception
    def convertColumnToInt(self, data, columnName):
        print(
            f'Transformation Util - convertColumnToInt - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - convertColumnToInt - converts column to pyspark.sql.types.IntegerType")

        try:
            convertedData = data.withColumn(columnName, data[columnName].cast("int"))
            insert_dynamo_record(self.job_name, "convertColumnToInt", self.job_environment,
                                 self.source_target_mapping, self.domain_name,
                                 {"selected_cols": [str(col) for col in data.dtypes],
                                  "source_tables": [id(data)],
                                  "final_df_cols": [str(col) for col in convertedData.dtypes],
                                  "target_tables": id(convertedData)})
            print("Transformation Util - convertColumnToInt - Finished")
            return convertedData
        except Exception:
            self.logger.error('Transformation Util - convertColumnToInt - Error while converting column into int')
            raise

    # converts column to pyspark.sql.types.StringType
    @logger_util.common_logger_exception
    def convertColumnToString(self, data, columnName):
        print(
            f'Transformation Util - convertColumnToString - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - convertColumnToString - converts column to pyspark.sql.types.StringType")

        try:
            convertedData = data.withColumn(columnName, data[columnName].cast("string"))
            insert_dynamo_record(self.job_name, "convertColumnToString", self.job_environment,
                                 self.source_target_mapping, self.domain_name,
                                 {"selected_cols": [str(col) for col in data.dtypes],
                                  "source_tables": [id(data)],
                                  "final_df_cols": [str(col) for col in convertedData.dtypes],
                                  "target_tables": id(convertedData)})
            print("Transformation Util - convertColumnToString - Finished")
            return convertedData
        except Exception:
            self.logger.error('Transformation Util - convertColumnToString - Error while converting column into string')
            raise

    # sorts data by keyColumns passed in as list
    @logger_util.common_logger_exception
    def sort(self, data, keyColumns, descOrder=False):
        print(
            f'Transformation Util - sort - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - sort - sorts data by keyColumns passed in as list")

        if not isinstance(data, DataFrame):
            self.logger.error('Transformation Util - sort - data passed in is not list')
            raise TypeError()

        if not isinstance(keyColumns, list):
            self.logger.error('Transformation Util - sort - keyColumns passed in is not list')
            raise TypeError()

        if not isinstance(descOrder, bool):
            self.logger.error('Transformation Util - sort - descOrder passed in is not bool')
            raise TypeError()

        try:
            sortedData = data.sort(keyColumns, ascending=not descOrder)
            insert_dynamo_record(self.job_name, "sort", self.job_environment,
                                 self.source_target_mapping, self.domain_name,
                                 {"selected_cols": [str(col) for col in data.dtypes],
                                  "source_tables": [id(data)],
                                  "final_df_cols": [str(col) for col in sortedData.dtypes],
                                  "target_tables": id(sortedData)})
            print("Transformation Util - sort - Finished")
            return sortedData
        except Exception:
            self.logger.error('Transformation Util - sort - Error while sorting data')
            raise

    # Group by the key field then return the first row within each group
    @logger_util.common_logger_exception
    def groupSelectFirst(self, data, keyColumns):
        print(
            f'Transformation Util - groupSelectFirst - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - groupSelectFirst - Group by the key&field then return the first row within each group")

        if not isinstance(data, pyspark.sql.dataframe.DataFrame):
            self.logger.error('Transformation Util - groupSelectFirst - data passed in is not DataFrame')
            raise TypeError()

        if not isinstance(keyColumns, list) and not isinstance(keyColumns, str):
            self.logger.error('Transformation Util - groupSelectFirst - keyColumns passed in is not list')
            raise TypeError()

        try:
            window = Window.orderBy(keyColumns).partitionBy(keyColumns)
            groupedData = data.withColumn("row", F.row_number().over(window)).filter(F.col("row") == 1).drop("row")
            insert_dynamo_record(self.job_name, "groupSelectFirst", self.job_environment,
                                 self.source_target_mapping, self.domain_name,
                                 {"selected_cols": [str(col) for col in data.dtypes],
                                  "source_tables": [id(data)],
                                  "final_df_cols": [str(col) for col in groupedData.dtypes],
                                  "target_tables": id(groupedData)})
            print("Transformation Util - groupSelectFirst - Finished")
            return groupedData
        except Exception:
            self.logger.error('Transformation Util - groupSelectFirst - Error while grouping data')
            raise

    # remove duplicate rows from the dataframe
    @logger_util.common_logger_exception
    def removeDuplicates(self, dataObject):
        print(
            f'Transformation Util - removeDuplicates - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - removeDuplicates - remove duplicate rows from the dataframe")

        if not isinstance(dataObject, DataFrame):
            raise TypeError('Transformation Util - removeDuplicates - dataObject passed in is not DataFrame')
        try:
            new_data = dataObject.distinct()
            insert_dynamo_record(self.job_name, "removeDuplicates", self.job_environment,
                                 self.source_target_mapping, self.domain_name,
                                 {"selected_cols": [str(col) for col in dataObject.dtypes],
                                  "source_tables": [id(dataObject)],
                                  "final_df_cols": [str(col) for col in new_data.dtypes],
                                  "target_tables": id(new_data)})
            print("Transformation Util - removeDuplicates - Finished")
            return new_data
        except Exception:
            self.logger.error('Transformation Util - removeDuplicates - Error while removing duplicates datasets')
            raise

    # construct a pivot table where the pivoted values are joined together in a comma-seperated list string
    @logger_util.common_logger_exception
    def pivotAggregateData(self, inputData, indexcol, column, values):
        print(
            f'Transformation Util - pivotAggregateData - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - pivotAggregateData - construct a pivot table")

        if not isinstance(inputData, DataFrame):
            raise TypeError('Transformation Util - pivotAggregateData - dataObject passed in is not DataFrame')

        if not isinstance(indexcol, str):
            raise TypeError('Transformation Util - pivotAggregateData - index passed in is not string')

        if not isinstance(column, str):
            raise TypeError('Transformation Util - pivotAggregateData - column passed in is not string')

        if not isinstance(values, str):
            raise TypeError('Transformation Util - pivotAggregateData - value passed in is not string')
        try:
            pivotedData = inputData.groupBy(indexcol)

            if pivotedData.count() == 0:
                pivotedData = inputData
            else:
                pivotedData = pivotedData.pivot(column).agg(F.concat_ws(',', F.collect_list(values)))
            insert_dynamo_record(self.job_name, "pivotAggregateData", self.job_environment,
                                 self.source_target_mapping, self.domain_name,
                                 {"selected_cols": [str(col) for col in inputData.dtypes],
                                  "source_tables": [id(inputData)],
                                  "final_df_cols": [str(col) for col in pivotedData.dtypes],
                                  "target_tables": id(pivotedData)})
            return pivotedData
        except Exception:
            self.logger.error('Transformation Util - pivotAggregateData - Error while pivoting datasets')
            raise

    # Construct a pivot table.
    @logger_util.common_logger_exception
    def pivotTable(self, inputData, indexcol, column, values, aggfunc):
        print(
            f'Transformation Util - pivotTable - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - pivotTable - construct a pivot table")

        if not isinstance(inputData, DataFrame):
            raise TypeError('Transformation Util - pivotTable - dataObject passed in is not DataFrame')

        if not isinstance(aggfunc, str):
            raise TypeError('Transformation Util - pivotTable - function passed in is not lambda')

        try:
            pivotedData = inputData.groupBy(indexcol)

            if pivotedData.count() == 0:
                pivotedData = inputData
            else:
                pivotedData = pivotedData.pivot(column).agg({values: aggfunc})
            insert_dynamo_record(self.job_name, "pivotTable", self.job_environment,
                                 self.source_target_mapping, self.domain_name,
                                 {"selected_cols": [str(col) for col in inputData.dtypes],
                                  "source_tables": [id(inputData)],
                                  "final_df_cols": [str(col) for col in pivotedData.dtypes],
                                  "target_tables": id(pivotedData)})
            return pivotedData
        except Exception:
            self.logger.error('Transformation Util - pivotTable - Error while pivoting datasets')
            raise

    @logger_util.common_logger_exception
    def sparkSql(self, sql_query, src_tables):
        print(
            f'Transformation Util - sparkSql - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - sparkSql - run a sql query using sparkSql")

        if not isinstance(sql_query, str):
            raise TypeError('Transformation Util - sparkSql - sql query passed in is not string')
        try:
            df = self.spark.sql(sql_query)
            insert_dynamo_record(self.job_name, "sparkSql", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"sql_text": sql_query, "final_df_cols": [str(col) for col in df.dtypes],
                                  "target_tables": id(df), "source_tables": src_tables})
            return df
        except Exception:
            self.logger.error('Transformation Util - sparkSql - Error while creating df using sql query')
            raise

    @logger_util.common_logger_exception
    def createTargetTable(self, pipeline_id, final_entity_df, final_path_out, app_name, aws_region):
        print(
            f'Transformation Util - createTargetTable - Started at {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        print(
            "Transformation Util - createTargetTable - create target table using athena util")

        if not isinstance(final_entity_df, DataFrame):
            raise TypeError('Transformation Util - createTargetTable - Data should be of type Dataframe')
        try:
            athena_util.AthenaUtil.create_athena_processed_table(pipeline_id, final_entity_df, final_path_out,
                                                                 app_name,
                                                                 self.domain_name, aws_region, self.job_environment)
            insert_dynamo_record(self.job_name, "Final", self.job_environment, self.source_target_mapping,
                                 self.domain_name,
                                 {"final_df_cols": [
                                     # str(col) if not col[1].startswith('decimal') else f"('{col[0]}', 'decimal')" for
                                     str(col) for col in final_entity_df.dtypes],
                                  "source_tables": [id(final_entity_df)],
                                  "target_tables": final_path_out.split('/')[-1] if len(
                                      final_path_out.split('/')[-1]) > 1 else final_path_out.split('/')[-2],
                                     "final_path": final_path_out})
            return
        except Exception:
            self.logger.error('Transformation Util - createTargetTable - Error while creating df using sql query')
            raise
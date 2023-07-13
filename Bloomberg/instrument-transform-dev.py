import sys

import pandas as pd
from datetime import date
from datetime import datetime

import pyspark
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as f
from pyspark.sql import Window
from functools import reduce
import boto3
import json
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
import requests

args = getResolvedOptions(sys.argv, ["JOB_NAME","AppName","JobEnvironment"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
AppName = args["AppName"]
JobEnvironment = args["JobEnvironment"]


def read_data(table_name):
    secret_manager = boto3.client(service_name='secretsmanager')
    secret = f'{AppName}/{JobEnvironment}/rds/{AppName}/pgs_{JobEnvironment}_{AppName}_master-secret'
    secret_value = secret_manager.get_secret_value(SecretId=secret)['SecretString']
    db_secret = json.loads(secret_value)
    connection = {"user": db_secret["username"],
                  "password": db_secret["password"],
                  "url": f"jdbc:postgresql://{db_secret['host']}/{db_secret['dbname']}",
                  "driver": "org.postgresql.Driver"}
    df = spark.read.format("jdbc").options(**connection).option("dbtable", table_name).load()
    return df


def collect_data():
    # df = pd.read_excel("bloomberg.xlsx")
    source_table = "instrument_ext.stg_index_bond_bloomberg"
    df = read_data(source_table)
    df = df.withColumn("MaturDate", f.to_timestamp(f.col("MaturDate"), 'yyyyMMdd'))

    return df


def daily_yield(df, ref_col="MoodysFCRating", weight_by="OutstandE", issuer_class="SOVEREIGN"):
    df_list = []
    old_df = df

    if issuer_class != "":
        old_df = old_df.filter(old_df["IssrClsL2"] == issuer_class)

    target_column = "YldMatE"
    weight_column = weight_by
    categories = [row[0] for row in old_df.select(ref_col).distinct().collect()]

    for category in categories:
        temp_df = old_df.filter(old_df[ref_col] == category)
        temp_df = temp_df.withColumn(f"Weighted Yield {ref_col}",
                                     temp_df[target_column] * (temp_df[weight_column] / f.sum(temp_df[weight_column])))
        df_list.append(temp_df)

    new_df = reduce(pyspark.sql.dataframe.DataFrame.union, df_list)
    agg_df = new_df.groupBy([ref_col]).sum(f"Weighted Yield {ref_col}").withColumnRenamed(
        f"sum(Weighted Yield {ref_col})", f"Weighted Yield {ref_col}").sort(f"Weighted Yield {ref_col}")
    # agg_df = agg_df[[f"Weighted Yield {ref_col}"]]
    # agg_df = agg_df.sort_values(by=[f"Weighted Yield {ref_col}"])

    return agg_df


def daily_spread(df, ref_col="MoodysFCRating", weight_by="OutstandE", issuer_class="SOVEREIGN"):
    df_list = []
    old_df = df

    if issuer_class != "":
        old_df = old_df.filter(old_df["IssrClsL2"] == issuer_class)

    target_column = "OAStoWrsE"
    weight_column = weight_by
    categories = [row[0] for row in old_df.select(ref_col).distinct().collect()]

    for category in categories:
        temp_df = old_df.filter(old_df[ref_col] == category)
        temp_df = temp_df.withColumn(f"Weighted OAS to Worst {ref_col}",
                                     temp_df[target_column] * (temp_df[weight_column] / f.sum(temp_df[weight_column])))
        df_list.append(temp_df)

    new_df = reduce(pyspark.sql.dataframe.DataFrame.union, df_list)
    agg_df = new_df.groupBy([ref_col]).sum(f"Weighted OAS to Worst {ref_col}").withColumnRenamed(
        f"sum(Weighted OAS to Worst {ref_col})", f"Weighted OAS to Worst {ref_col}").sort(
        f"Weighted OAS to Worst {ref_col}")
    # agg_df = agg_df[[f"Weighted OAS to Worst {ref_col}"]]
    # agg_df = agg_df.sort_values(by=[f"Weighted OAS to Worst {ref_col}"])

    return new_df, agg_df


def awm_debt(df, ref_col="MoodysFCRating", weight_by="OutstandE", issuer_class="SOVEREIGN"):
    df_list = []
    old_df = df

    if issuer_class != "":
        old_df = old_df.filter(old_df["IssrClsL2"] == issuer_class)

    target_column = "Maturity"
    weight_column = weight_by
    categories = [row[0] for row in old_df.select(ref_col).distinct().collect()]

    for category in categories:
        temp_df = old_df.filter(old_df[ref_col] == category)
        temp_df = temp_df.withColumn(f"Weighted Maturity {ref_col}",
                                     temp_df[target_column] * (temp_df[weight_column] / f.sum(temp_df[weight_column])))
        df_list.append(temp_df)

    new_df = reduce(pyspark.sql.dataframe.DataFrame.union, df_list)
    agg_df = new_df.groupBy([ref_col]).sum(f"Weighted Maturity {ref_col}").withColumnRenamed(
        f"sum(Weighted Maturity {ref_col})", f"Weighted Maturity {ref_col}").sort(f"Weighted Maturity {ref_col}")
    # agg_df = agg_df[[f"Weighted Maturity {ref_col}"]]
    # agg_df = agg_df.sort_values(by=[f"Weighted Maturity {ref_col}"])

    return new_df, agg_df


def maturing_debt_data(df, ref_col="MoodysFCRating", time_periods=[12, 24, 36]):  # time period value is months

    # We need to get Maturing Debt/Total Debt and WEA Yield of Maturing Debt
    # Overall outstanding amount  / share of debt maturing
    # over the next 12 months / 24 months and 36 months (by sovereign, region, rating level)
    # (by sovereign, region, rating level)

    # get current total values
    Total_Debt_df = df.groupby(by=[ref_col]).sum("OutstandE").\
        withColumnRenamed("sum(OutstandE)", "Total USD Debt Outstanding")
    # Total_Debt_df = Total_Debt_df[["OutstandE"]]
    # Total_Debt_df = Total_Debt_df.rename(columns={"OutstandE":"Total USD Debt Outstanding"}) #this is the total debt in the database
    # rating_list = Total_Debt_df.index.tolist()
    # print(rating_list)

    # get subset values by time periods
    current_date = date.today()
    sub_df_list = []
    sub_yield_df_list = []

    for times in time_periods:
        future_date = current_date + relativedelta(months=times)
        sub_df = df.filter(f.col("MaturDate") <= f.to_timestamp(str(future_date)))

        # get the amount of debt outstanding within the designated time period

        sub_agg_df = sub_df.groupby(by=[ref_col]).sum("OutstandE").\
            withColumnRenamed("sum(OutstandE)", "Debt Maturing Within {times} months")
        # sub_agg_df = sub_agg_df[["OutstandE"]]
        # sub_agg_df = sub_agg_df.rename(columns={"OutstandE":f"Debt Maturing Within {times} months"})
        sub_df_list.append(sub_agg_df)

        # get the weighted average yield of the maturing debt

        __, yield_sub_agg_df = daily_yield(df=sub_df, ref_col=ref_col)
        yield_sub_agg_df = yield_sub_agg_df.withColumnRenamed(f"Weighted Yield {ref_col}",
                                                              f"Weighted Yield of Debt Maturing within {times} Months")
        sub_df_list.append(yield_sub_agg_df)

    # combine into simple dataframe

    final_df = Total_Debt_df
    # print(Total_Debt_df)
    for df in sub_df_list:
        # print(df)
        final_df = final_df.union(df)

    # make final calculations

    final_df = final_df.withColumn("Portion of Debt Maturing in 12 months",
                                   final_df["Debt Maturing Within 12 months"] / final_df[
                                       "Total USD Debt Outstanding"]).withColumn(
        "Portion of Debt Maturing in 24 months",
        final_df["Debt Maturing Within 24 months"] / final_df["Total USD Debt Outstanding"]).withColumn(
        "Portion of Debt Maturing in 36 months",
        final_df["Debt Maturing Within 36 months"] / final_df["Total USD Debt Outstanding"])

    # return dataframe values

    return final_df


def outstanding_debt(df):
    pass


def get_data():
    bb_df = collect_dataframes()
    # Sov - Issuer, Region - Region, Rating - QualityB

    # get the daily yield information
    __, sov_yield = daily_yield(df=bb_df, ref_col="MoodysCountry")
    __, region_yield = daily_yield(df=bb_df, ref_col="MoodysRegion")
    __, rating_yield = daily_yield(df=bb_df)

    # get the daily spread information
    __, sov_spread = daily_spread(df=bb_df, ref_col="MoodysCountry")
    __, region_spread = daily_spread(df=bb_df, ref_col="MoodysRegion")
    __, rating_spread = daily_spread(df=bb_df)

    # get the average weighted maturing data
    __, sov_awm = awm_debt(df=bb_df, ref_col="MoodysCountry")
    __, region_awm = awm_debt(df=bb_df, ref_col="MoodysRegion")
    __, rating_awm = awm_debt(df=bb_df)

    # get time based data

    rating_time = maturing_debt_data(df=bb_df)
    region_time = maturing_debt_data(df=bb_df, ref_col="MoodysRegion")
    country_time = maturing_debt_data(df=bb_df, ref_col="MoodysCountry")

    # combine all the data

    sov_df_list = [sov_yield, sov_spread, sov_awm, country_time]
    sov_df = sov_df_list[0]
    for df in sov_df_list[1:]:
        sov_df = sov_df.union(df)

    region_df_list = [region_yield, region_spread, region_awm, region_time]
    region_df = region_df_list[0]
    for df in region_df_list[1:]:
        region_df = region_df.union(df)

    rating_df_list = [rating_yield, rating_spread, rating_awm, rating_time]
    rating_df = rating_df_list[0]
    for df in rating_df_list[1:]:
        rating_df = rating_df.union(df)

    todays_date = datetime.today().strftime('%d%m%Y')
    file_name = f"{todays_date} Data File.xlsx"

    # write the final dataframes to posr

    with pd.ExcelWriter(file_name) as writer:

        # use to_excel function and specify the sheet_name and index
        # to store the dataframe in specified sheet

        sov_df.to_excel(writer, sheet_name="Sov Data")
        region_df.to_excel(writer, sheet_name="Region Data")
        rating_df.to_excel(writer, sheet_name="Rating Data")


def get_org_info():
    url = "http://ptc-wbmistap103:8010/expose-data"

    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'
    }

    data = {
        "ApplicationId": "17",
        "RequestId": "1",
        "Output": {"OutputType": "JSON", "FileType": "JSON"}
    }

    res = requests.post(url=url, json=data, headers=headers)
    res_data = res.json()
    df = pd.DataFrame(data=res_data)
    rename_columns = {'SchemaVersion': 'Schema_Version', 'SourceOperation': 'Source_Operation',
                      'SourceOperationDateTime': 'Source_Operation_DateTime',
                      'OrganizationIdentifier': 'Organization_Identifier', 'OrganizationName': 'Organization_Name',
                      'IssuerRatingType': 'Issuer_Rating_Type', 'Region': 'Region',
                      'EconomyTypeIdentifier': 'Economy_Type_Identifier', 'EconomyTypeCode': 'Economy_Type_Code',
                      'EconomyTypeName': 'Economy_Type_Name', 'EconomyTypeDescription': 'Economy_Type_Description',
                      'LCCountryCeiling': 'LCCountry_Ceiling', 'FCCountryCeiling': 'FCCountry_Ceiling',
                      'LCSovereignRating': 'LCSovereign_Rating', 'FCSovereignRating': 'FCSovereign_Rating',
                      'Outlook': 'Outlook', 'ExchangeRateRegimeIdentifier': 'Exchange_Rate_Regime_Identifier',
                      'ExchangeRateRegimeCode': 'Exchange_Rate_Regime_Code',
                      'ExchangeRateRegimeName': 'Exchange_Rate_Regime_Name',
                      'ExchangeRateRegimeDescription': 'Exchange_Rate_Regime_Description',
                      'CurrencyCode': 'Currency_Code', 'CurrencyName': 'Currency_Name',
                      'CurrencyDescription': 'Currency_Description', 'Footnote': 'Footnote'}
    df.rename(columns=rename_columns, inplace=True)
    # load_postgres_data(df, "SovFM_Data_Dictionary", secret_name)
    df.to_excel("Org info.xlsx", index=False)

    return df


def get_acc_info():
    url = "http://ptc-wbmistap103:8010/expose-data"

    headers = {
        'Content-type': 'application/json',
        'Accept': 'application/json'
    }

    data = {
        "ApplicationId": "17",
        "RequestId": "2",
        "Output": {"OutputType": "JSON", "FileType": "JSON"}
    }

    res = requests.post(url=url, json=data, headers=headers)
    res_data = res.json()
    acct_info_df = pd.DataFrame(data=res_data)
    print(df.columns)
    # load_postgres_data(df, "SovFM_Data_Dictionary", secret_name)
    # df.to_excel("SovFM_Data_Dictionary.xlsx", index=False)

    return df


def collect_dataframes():
    # collect all the relevant dataframes

    org_df = get_org_info()
    acc_info = get_acc_info()
    bloomberg_df = collect_data()
    # bloomberg_df.show()
    mapping_df = pd.read_excel("countrymapping.xlsx")

    # create mapping dictionary
    country = mapping_df.Country.tolist()
    moody_names = mapping_df.OrganizationName.tolist()
    mapping_dict = {}

    for country, org_name in zip(country, moody_names):  ##
        mapping_dict[country] = org_name

        # add moody's info to the bloomberg dataframe
    bloomberg_country = [cntry[0] for cntry in bloomberg_df.select("Country").collect()]
    moodys_country = []
    moodys_region = []
    moodys_fc_rating = []
    SovFM_GG_Debt_USD = []
    SovFM_GG_DebtGDP = []

    for country in bloomberg_country:
        moodys_country = mapping_dict[country]
        subset = org_df.filter(org_df["OrganizationName"] == moodys_count)
        region = subset.select("Region").distinct().collect()[0][0]
        fc_rating = subset.select("FCSovereignRating").distinct().collect()[0][0]
        moodys_reg = region
        moodys_fc = fc_rating
        # df_gg_debt = data_connector.find_all_related_data(moodys_count,"Gen. gov. debt (US$ bil.)", 2023, 2023)["NativeAmount"][0]
        # df_gg_debt_gdp = data_connector.find_all_related_data(moodys_count,"Gen. gov. debt/GDP", 2023, 2023)["NativeAmount"][0]
        # SovFM_GG_Debt_USD.append(df_gg_debt)
        # SovFM_GG_DebtGDP.append(df_gg_debt_gdp)
        moodys_country.append(moodys_country)
        moodys_region.append(moodys_reg)
        moodys_fc_rating.append(moodys_fc)

    moodys_df = spark.createDataFrame(zip(moodys_country, moodys_region, moodys_fc_rating),
                                      (["MoodysCountry", "MoodysRegion", "MoodysFCRating"]))

    moodys_df = moodys_df.withColumn("row_idx", f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
    bloomberg_df = bloomberg_df.withColumn("row_idx",
                                           f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))

    bloomberg_df = bloomberg_df.join(moodys_df, "row_idx").drop("row_idx")

    # bloomberg_df["Moodys_GG_Debt_USD_2023"] = SovFM_GG_Debt_USD
    # bloomberg_df["Moodys_GG_DebtGDP_2023"] = SovFM_GG_DebtGDP

    return bloomberg_df


def main():
    get_data()
    pass


if __name__ == "__main__":
    main()
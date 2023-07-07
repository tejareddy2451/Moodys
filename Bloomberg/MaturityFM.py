import pandas as pd
from datetime import date
from datetime import datetime
from SovFM import SovFM
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as f
from pyspark.sql import Window
from functools import reduce

def read_data(spark, table_name):

    df = spark.read.format("jdbc").options(connection).option("dbtable", "").load()
    return df

def collect_data():

    # df = pd.read_excel("bloomberg.xlsx")
    df = read_data(spark, source_table)
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

    for cat in categories:
        temp_df = old_df.filter(old_df[ref_col]==cat)
        temp_df = temp_df.withColumn(f"Weighted Yield {ref_col}", temp_df[target_column] * (temp_df[weight_column]/f.sum(temp_df[weight_column])))
        df_list.append(temp_df)

    new_df = reduce(pyspark.sql.dataframe.DataFrame.union, df_list)
    agg_df = new_df.groupBy([ref_col]).sum(f"Weighted Yield {ref_col}").withColumnRenamed(f"sum(Weighted Yield {ref_col})", f"Weighted Yield {ref_col}").sort(f"Weighted Yield {ref_col}")
    #agg_df = agg_df[[f"Weighted Yield {ref_col}"]]
    #agg_df = agg_df.sort_values(by=[f"Weighted Yield {ref_col}"])

    return new_df, agg_df

def daily_spread(df, ref_col="MoodysFCRating", weight_by="OutstandE", issuer_class="SOVEREIGN"):
    df_list = []
    old_df = df 

    if issuer_class != "":
        old_df = old_df.filter(old_df["IssrClsL2"] == issuer_class)

    target_column = "OAStoWrsE"
    weight_column = weight_by
    categories = [row[0] for row in old_df.select(ref_col).distinct().collect()]

    for cat in categories:
        temp_df = old_df.filter(old_df[ref_col]==cat)
        temp_df = temp_df.withColumn(f"Weighted OAS to Worst {ref_col}", temp_df[target_column] * (temp_df[weight_column]/f.sum(temp_df[weight_column])))
        df_list.append(temp_df)

    new_df = reduce(pyspark.sql.dataframe.DataFrame.union, df_list)
    agg_df = new_df.groupBy([ref_col]).sum(f"Weighted OAS to Worst {ref_col}").withColumnRenamed(f"sum(Weighted OAS to Worst {ref_col})", f"Weighted OAS to Worst {ref_col}").sort(f"Weighted OAS to Worst {ref_col}")
    #agg_df = agg_df[[f"Weighted OAS to Worst {ref_col}"]]
    #agg_df = agg_df.sort_values(by=[f"Weighted OAS to Worst {ref_col}"])

    return new_df, agg_df

def awm_debt(df, ref_col="MoodysFCRating", weight_by="OutstandE", issuer_class="SOVEREIGN"):
    df_list = []
    old_df = df 

    if issuer_class != "":
        old_df = old_df[old_df["IssrClsL2"] == issuer_class] 

    target_column = "Maturity"
    weight_column = weight_by
    categories = old_df[ref_col].unique().tolist()

    for cat in categories:
        temp_df = old_df[old_df[ref_col]==cat]
        temp_df[f"Weighted Maturity {ref_col}"] = temp_df[target_column] * (temp_df[weight_column]/temp_df[weight_column].sum())
        df_list.append(temp_df)

    new_df = pd.concat(df_list)
    agg_df = new_df.groupby(by=[ref_col]).sum()
    agg_df = agg_df[[f"Weighted Maturity {ref_col}"]]
    agg_df = agg_df.sort_values(by=[f"Weighted Maturity {ref_col}"])

    return new_df, agg_df

def maturing_debt_data(df, ref_col="MoodysFCRating", time_periods=[12, 24, 36]): #time period value is months 

    # We need to get Maturing Debt/Total Debt and WEA Yield of Maturing Debt 
    # Overall outstanding amount  / share of debt maturing  
    # over the next 12 months / 24 months and 36 months (by sovereign, region, rating level) 
    # (by sovereign, region, rating level)
    
    #get current total values
    Total_Debt_df = df.groupby(by=[ref_col]).sum()
    Total_Debt_df = Total_Debt_df[["OutstandE"]]
    Total_Debt_df = Total_Debt_df.rename(columns={"OutstandE":"Total USD Debt Outstanding"}) #this is the total debt in the database
    rating_list = Total_Debt_df.index.tolist()
    print(rating_list)

    #get subset values by time periods 
    current_date = date.today()
    sub_df_list = []
    sub_yield_df_list = []

    for times in time_periods:
        future_date = current_date + relativedelta(months=times)
        sub_df  = df[df["MaturDate"] <= pd.to_datetime(future_date)]

        #get the amount of debt outstanding within the designated time period

        sub_agg_df = sub_df.groupby(by=[ref_col]).sum()
        sub_agg_df = sub_agg_df[["OutstandE"]]
        sub_agg_df = sub_agg_df.rename(columns={"OutstandE":f"Debt Maturing Within {times} months"})
        sub_df_list.append(sub_agg_df)

        #get the weighted average yield of the maturing debt

        __, yield_sub_agg_df = daily_yield(df=sub_df,ref_col=ref_col)
        yield_sub_agg_df = yield_sub_agg_df.rename(columns={f"Weighted Yield {ref_col}":f"Weighted Yield of Debt Maturing within {times} Months"})
        sub_df_list.append(yield_sub_agg_df)

    #combine into simple dataframe

    final_df = Total_Debt_df
    #print(Total_Debt_df)
    for df in sub_df_list:
        #print(df)
        final_df = pd.concat([final_df, df], axis=1)

    #make final calculations

    final_df["Portion of Debt Maturing in 12 months"] = final_df["Debt Maturing Within 12 months"] / final_df["Total USD Debt Outstanding"]
    final_df["Portion of Debt Maturing in 24 months"] = final_df["Debt Maturing Within 24 months"] / final_df["Total USD Debt Outstanding"]
    final_df["Portion of Debt Maturing in 36 months"] = final_df["Debt Maturing Within 36 months"] / final_df["Total USD Debt Outstanding"]

    #return dataframe values

    return final_df

def outstanding_debt(df):
    pass

def get_data():
    bb_df = combine_data()
    # Sov - Issuer, Region - Region, Rating - QualityB

    #get the daily yield information
    __, sov_yield = daily_yield(df=bb_df, ref_col="MoodysCountry")
    __, region_yield = daily_yield(df=bb_df, ref_col="MoodysRegion")
    __, rating_yield = daily_yield(df=bb_df)

    #get the daily spread information
    __, sov_spread = daily_spread(df=bb_df, ref_col="MoodysCountry")
    __, region_spread = daily_spread(df=bb_df, ref_col="MoodysRegion")
    __, rating_spread = daily_spread(df=bb_df)

    #get the average weighted maturing data
    __, sov_awm = awm_debt(df=bb_df, ref_col = "MoodysCountry")
    __, region_awm = awm_debt(df=bb_df, ref_col="MoodysRegion")
    __, rating_awm = awm_debt(df=bb_df)

    #get time based data

    rating_time = maturing_debt_data(df=bb_df)
    region_time = maturing_debt_data(df=bb_df,ref_col="MoodysRegion")
    country_time = maturing_debt_data(df=bb_df,ref_col="MoodysCountry")

    #combine all the data

    sov_df_list = [sov_yield, sov_spread, sov_awm, country_time]
    sov_df = sov_df_list[0]
    for df in sov_df_list[1:]:
        sov_df = pd.concat([sov_df, df], axis=1)

    region_df_list = [region_yield, region_spread, region_awm, region_time]
    region_df = region_df_list[0]
    for df in region_df_list[1:]:
        region_df = pd.concat([region_df, df], axis=1)

    rating_df_list = [rating_yield, rating_spread, rating_awm, rating_time]
    rating_df = rating_df_list[0]
    for df in rating_df_list[1:]:
        rating_df = pd.concat([rating_df, df], axis=1)

    todays_date = datetime.today().strftime('%d%m%Y')
    file_name = f"{todays_date} Data File.xlsx"

    with pd.ExcelWriter(file_name) as writer:
   
        # use to_excel function and specify the sheet_name and index
        # to store the dataframe in specified sheet

        sov_df.to_excel(writer, sheet_name="Sov Data")
        region_df.to_excel(writer, sheet_name="Region Data")
        rating_df.to_excel(writer, sheet_name="Rating Data")

def combine_data():

    #collect all the relevant dataframes

    data_connector = SovFM()
    org_df = data_connector.org_info
    bloomberg_df = collect_data()
    mapping_df = pd.read_excel("countrymapping.xlsx")

    #create mapping dictionary 
    country = mapping_df.Country.tolist()
    moody_names = mapping_df.OrganizationName.tolist()
    mapping_dict = {}

    for count, name in zip(country, moody_names):
        mapping_dict[count] = name 

    #add moody's info to the bloomberg dataframe
    bloomberg_country = [cntry[0] for cntry in bloomberg_df.select("Country").collect()]
    moodys_country = []
    moodys_region = []
    moodys_fc_rating = []
    SovFM_GG_Debt_USD = []
    SovFM_GG_DebtGDP = []


    for count in bloomberg_country:
        moodys_count = mapping_dict[count]
        subset = org_df.filter(org_df["OrganizationName"] == moodys_count)
        region = subset.select("Region").distinct().collect()[0][0] 
        fc_rating = subset.select("FCSovereignRating").distinct().collect()[0][0]
        moodys_reg = region
        moodys_fc = fc_rating
        #df_gg_debt = data_connector.find_all_related_data(moodys_count,"Gen. gov. debt (US$ bil.)", 2023, 2023)["NativeAmount"][0]
        #df_gg_debt_gdp = data_connector.find_all_related_data(moodys_count,"Gen. gov. debt/GDP", 2023, 2023)["NativeAmount"][0]
        #SovFM_GG_Debt_USD.append(df_gg_debt)
        #SovFM_GG_DebtGDP.append(df_gg_debt_gdp)
        moodys_country.append(moodys_count)
        moodys_region.append(moodys_reg)
        moodys_fc_rating.append(moodys_fc)

    moodys_df = spark.createDataFrame(zip(moodys_country, moodys_region, moodys_fc_rating),(["MoodysCountry", "MoodysRegion", "MoodysFCRating"]))
    
    moodys_df = moodys_df.withColumn("row_idx", f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
    bloomberg_df = bloomberg_df.withColumn("row_idx", f.row_number().over(Window.orderBy(f.monotonically_increasing_id())))
    
    bloomberg_df = bloomberg.join(moodys_df, "row_idx").drop("row_idx")

    #bloomberg_df["Moodys_GG_Debt_USD_2023"] = SovFM_GG_Debt_USD
    #bloomberg_df["Moodys_GG_DebtGDP_2023"] = SovFM_GG_DebtGDP

    return bloomberg_df


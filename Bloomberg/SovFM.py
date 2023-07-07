from re import L
import requests
import pandas as pd
from tabulate import tabulate

class SovFM(object):
    def __init__(self):
        self.org_info = self.get_org_info()
        self.acc_info = self.get_acc_info()

    def get_country_names(self):
        country_names = self.org_info["OrganizationName"].tolist()
        data = []
        header_row = [1, 2, 3]
        data.append(header_row)
        country_num = 0
        temp_list = []

        for num in range(len(country_names)):
            if country_num % 3 == 0 and country_num != 0:
                data.append(temp_list)
                temp_list = []
                country_num = 0
            else:
                temp_list.append(country_names[num])
                country_num += 1

        data.append(temp_list)

        print(tabulate(data, headers="firstrow",tablefmt="fancy_grid"))

        return country_names

    def get_indicator_names(self,keys=[]):
        indicator_names = self.acc_info["AccountName"].tolist()
        data = []
        header_row = ["Indicators"]
        data.append(header_row)
        indicator_num = 0
        temp_list = []

        for num in range(len(indicator_names)):
            if keys == []:
                data.append([indicator_names[num][:100]])
            else:
                for key in keys:
                    if key in indicator_names[num] and [indicator_names[num]] not in data:
                        data.append([indicator_names[num][:100]])
            
        print(tabulate(data, headers="firstrow",tablefmt="fancy_grid"))

        return data[1:]

    def get_indicator_id(self, indicator="",switch=False):
        df = self.acc_info
        sub_df = df[df["AccountName"]==indicator]
        indicator_ids = sub_df["AccountIdentifier"].tolist()

        if indicator_ids == [] and not switch:
            print("You may need to add a space after the indicator name to get the Account ID!")
            indicator = indicator + " "
            return self.get_indicator_id(indicator, switch=True)

        return indicator_ids
    
    def get_org_id(self, org_name=""):
        df = self.org_info
        sub_df = df[df["OrganizationName"]==org_name]
        org_ids = sub_df["OrganizationIdentifier"].tolist()

        return org_ids

    def get_org_info(self):
        url = "http://ptc-wbmistap103:8010/expose-data"

        headers = {
            'Content-type':'application/json', 
            'Accept':'application/json'
            }

        data = {
            "ApplicationId": "17",
            "RequestId": "1",
            "Output":{"OutputType": "JSON","FileType": "JSON"}
            }

        res = requests.post(url=url,json=data,headers=headers)
        res_data = res.json()
        df = pd.DataFrame(data=res_data)
        df.to_excel("Org info.xlsx",index=False)

        return df

    def get_acc_info(self):
        url = "http://ptc-wbmistap103:8010/expose-data"

        headers = {
            'Content-type':'application/json', 
            'Accept':'application/json'
            }

        data = {
            "ApplicationId": "17",
            "RequestId": "2",
            "Output":{"OutputType":"JSON","FileType": "JSON"}
            }

        res = requests.post(url=url,json=data,headers=headers)
        res_data = res.json()
        df = pd.DataFrame(data=res_data)
        df.to_excel("SovFM_Data_Dictionary.xlsx",index=False)

        return df

    def get_series_info(self, org_id, account_id, start_year, end_year):
        url = "http://ptc-wbmistap103:8010/expose-data"
        headers = {
            'Content-type':'application/json', 
            'Accept':'application/json'
            }

        data = {
            "ApplicationId": "17",
            "RequestId": "3",
            "Param": {"OrganizationIdentifier":org_id,"FromFiscalYear": start_year,"ToFiscalYear": end_year, "AccountID":account_id},
            "Output":{"OutputType":"JSON","FileType": "JSON"}
            }

        #body <-  noquote(paste0('{"ApplicationId": "17","RequestId": "3","Param":{"OrganizationIdentifier\":"',sel.org,'\",\"FromFiscalYear": "1995","ToFiscalYear": "2024"},"Output":{"OutputType": "JSON","FileType": "JSON"}}'))
        res = requests.post(url=url,json=data,headers=headers)
        res_data = res.json()
        if res_data == []:
            #print(res)
            empty_df = pd.DataFrame()
            return empty_df
        else:
            #print(res_data)
            #print(res_data[0]["AccountData"])
            OrgName = res_data[0]['OrganizationName']
            CurrencyName = res_data[0]['CurrencyName']
            df_list = []
            for account_data in res_data[0]["AccountData"]:
                AccountName = account_data['AccountName']
                AccountID = account_data['AccountIdentifier']
                temp_df = pd.DataFrame(data=account_data["Data"])
                temp_df["AccountName"] = [AccountName] * len(temp_df)
                temp_df["AccountID"] = [AccountID] * len(temp_df)
                temp_df["Org Name"] = [OrgName] * len(temp_df)
                temp_df["CurrencyName"] = [CurrencyName] * len(temp_df)
                df_list.append(temp_df)
            df = pd.concat(df_list)
            #df.to_excel(f"{org_id} {account_id}.xlsx",index=False)

        #print(df)
        return df

    def find_all_related_data(self, country_name, partial_account_name, start_year, end_year):
        org_id = self.get_org_id(country_name)[0]
        indicator_names = self.get_indicator_names(keys=[partial_account_name])
        df_list = []
        
        print(org_id)
        for name in indicator_names:
            #print(name)
            indicator_id = self.get_indicator_id(name[0])
            for id in indicator_id:
                #print(name, id)
                try:
                    df = self.get_series_info(org_id,id,start_year,end_year)
                    df_list.append(df)
                    #print(df)
                    #print(name, id)
                except:
                    print("Did not work.")

        #print(df_list)
        final_df = pd.concat(df_list)

        return final_df

    def get_ind_global(self):
        pass

    def get_all_indicators_one_sovereign(self):
        pass

    def get_mult_countries(self, org_id_list, account_id_list, start_year, end_year):
        df_list = []
        bad_list = []
        for org_id in org_id_list:
            for account_id in account_id_list:
                print(org_id, account_id)
                try:
                    temp_df = self.get_series_info(org_id, account_id, start_year, end_year)
                    if len(temp_df) == 0:
                        pass
                    else:
                        df_list.append(temp_df)
                except:
                    bad_list.append(org_id)

        final_df = pd.concat(df_list)
        #final_df.to_excel("Mult Country Data.xlsx", index=False)

        return final_df, bad_list

if __name__ == "__main__":
    data_connector = SovFM()
    #Gen. gov. debt (US$ bil.)
    #Gen. gov. debt/GDP
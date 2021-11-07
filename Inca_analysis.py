import pandas as pd
import numpy as np

# load and read the csv files
df1 = pd.read_csv("/home/bilal/inca/download_data_fincen_files/download_transactions_map.csv")
pd.set_option("display.max_columns", 20)
df_2015 = pd.read_csv("/home/bilal/inca/Bitcoin-large-transactions-2015_2016_2017/2015.csv")
df_2016 = pd.read_csv("/home/bilal/inca/Bitcoin-large-transactions-2015_2016_2017/2016.csv")
df_2017 = pd.read_csv("/home/bilal/inca/Bitcoin-large-transactions-2015_2016_2017/2017.csv")

# data cleaning: remove unnecessary columns
df1.drop(["originator_iso", "beneficiary_bank_id", "beneficiary_iso", "filer_org_name_id", "originator_bank_id", "end_date"],
         axis=1, inplace=True)
#df1 = df1.fillna(0)# Fill missed values

# to match the BTC blockchain hashes with the bank transaction, means merge the  datasets, we can do it by joining them
#with common columns which is the time and begin_date:

# transform begin date of our dataset df1 to datetime format, for that I created a dictionary where I transforme the string format
# of months to numerical format then from numerical to datetime format

dictionary_months = {"Jan": "01", "Feb":"02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06", "Jul":"07", "Aug":"08",
                     "Sep": "09", "Oct": "10","Nov": "11", "Dec": "12"}

#1 Transforming months to numerical format
list = df1["begin_date"]

lst = []

for string in list:
    a = str(string)
    b = a.replace(",", " ")
    lst.append(b.split())

lst2 = []
for index in np.arange(len(lst)):
    for month in dictionary_months:
        if lst[index][0] == month:
            lst[index][0] = dictionary_months[month]
    lst2.append("/".join(lst[index]))


df1["begin_date"] = lst2
#transforming to datetime format
df1["begin_date"] = pd.to_datetime(df1["begin_date"], format="%m/%d/%Y")

#print(df1["begin_date"])
#I have to rename begin_date by time to concatenate it with other datasets by column time
df1.rename({"begin_date": "time"}, axis=1, inplace=True)
#print(df1.head())
#print(df1.info())
#print(df2.head())
#transform the time column format of our 2015.2016, 2017 csv  to datetime format to concatinate our dataset with because before
# the data type of time was object type
df_2015["time"] = pd.to_datetime(df_2015["time"])
df_2016["time"] = pd.to_datetime(df_2016["time"])
df_2017["time"] = pd.to_datetime(df_2017["time"])

# creating a list of bitcoin transaction datasets so as to concatinate them in one and merge it with the transaction map
#dataset
datasets = [df_2015, df_2016, df_2017]
df_merged = pd.concat(datasets)
#df3 = df3.fillna('na')
#print(df_merged.head())
df3 = pd.merge(df1, df_merged)
#print(df3.head())
#df3.info()

df3.drop_duplicates(subset="id", keep="first", inplace=True)
# below, I have just ordered the columns for easy reading and understanding the transactions
df3 = df3.iloc[:, [3,0,1,4,2,6,13,15,14,11,12,10,5,7,8,9]]
print(df3.head())
# export and save to csv file
df3.to_csv(r"/home/bilal/inca/sus_bank&blckchain_tr.csv")

#calculating data correlations between numerical values:
print("correlation between the transaction  BTC and the transaction USD is: ",
      df3["Transaction_amount_BTC"].corr(df3["Transaction_amount_USD"]))
print("correlation between transaction BTC and Price is: ", df3["Transaction_amount_BTC"].corr(df3["Price"]))
print("correlation between transaction BTC and Price is:", df3["Transaction_amount_USD"].corr(df3["Price"]))


df3.info()






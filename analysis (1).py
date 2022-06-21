# Databricks notebook source
import pandas as pd
import numpy as np
import re

# COMMAND ----------

# df = pd.read_excel("/dbfs/mnt/terence/hase_openrice/filtered_hase_name_trx.xlsx", engine='openpyxl')
# trx = pd.read_csv("/dbfs/mnt/terence/hase_openrice/hase_trx_0428.csv")
merged_df = pd.read_csv("/dbfs/mnt/terence/hase_openrice/merged.csv")

# COMMAND ----------

merged_df.head()

# COMMAND ----------

wuxiforeign_payment_list = ['sq*', 'doordash*', 'paypal*', 'tst*', 'ztl*', 'iz*', 'ghl*', 'crv*', 'iz *', 'sq *', 'paypal *']

def clean(string):
    """Takes an input string, cleans it. 
    """
    string = str(string)
    string = string.lower() # lower case
    for prefix in foreign_payment_list:
        if prefix in string:
            string = "removing"
            return string
    return string

# COMMAND ----------

print(trx['sales'].sum())

# COMMAND ----------

print(len(trx['member_id'].unique()))

# COMMAND ----------

df['clean_hase_name'] = df['hase_name'].apply(lambda x: clean(x))

# COMMAND ----------

df2 = df.loc[df['clean_hase_name'] != 'removing']

# COMMAND ----------

tx2 = df2['count'].sum()
print(tx2)

# COMMAND ----------

trx.head()

# COMMAND ----------

final = pd.merge(trx, df2, left_on='hase_desc', right_on='hase_name', how='inner')

# COMMAND ----------

final.head()

# COMMAND ----------

# final[final.hase_desc == '% ARABICA KENNEDY TOWN'].sum()  
# print(len(final['member_id'].unique()))
# print(final['trx'].sum())
print(final['sales'].sum())
# print(len(final['hase_desc'].unique()))



# COMMAND ----------

tx2 = final['trx'].sum()
print(tx2)

# COMMAND ----------

matched = pd.read_csv("/dbfs/mnt/terence/hase_openrice/matched_table.csv")

# COMMAND ----------

merged_df = pd.merge(final, matched, left_on='hase_name', right_on='hase_name', how='inner')

# COMMAND ----------

merged_df.head(1)

# COMMAND ----------

merged_df = merged_df[['member_id', 'sales', 'trx', 'clean_hase_name', 'openrice_name', 'cuisine_type', 'district', 'subdistrict']]

# COMMAND ----------

merged_df.head(1)

# COMMAND ----------

print(len(merged_df['clean_hase_name'].unique()))
print(len(merged_df['member_id'].unique()))
print(merged_df['trx'].sum())
print(merged_df['sales'].sum())
print(len(merged_df['openrice_name'].unique()))
# print(len(final['hase_desc'].unique()))


# COMMAND ----------

print(len(merged_df['clean_hase_name'].unique()))

# COMMAND ----------

tx = df2['count'].sum()
print(tx)

# COMMAND ----------

matched.head()

# COMMAND ----------

merged_df = pd.merge(df2, matched, left_on='hase_name', right_on='hase_name', how='inner')

# COMMAND ----------

merged_df.head(2)

# COMMAND ----------

cuisine_df = pd.read_csv("/dbfs/mnt/terence/hase_openrice/cuisine_type_matched_list.csv")

# COMMAND ----------

cuisine_list = cuisine_df['moroccan'].tolist()

# COMMAND ----------

cuisine_list.append('moroccan')

# COMMAND ----------

len(cuisine_list)

# COMMAND ----------

for i in cuisine_list:
  merged_df[i] = merged_df['cuisine_type'].str.contains(i)

merged_df

# COMMAND ----------

merged_df.shape

# COMMAND ----------

filename = "/dbfs/mnt/terence/hase_openrice/merged.csv"
merged_df.to_csv(filename, index=False, sep=',')

# COMMAND ----------

merged_df.groupby(['openrice_name']).sum()

# COMMAND ----------

cuisine_list[0]

# COMMAND ----------

arr = []
for i in cuisine_list:
  Dict = {}
  test = merged_df[merged_df[i] == True]
  Dict['member'] = test['member_id'].tolist()
  Dict['trx'] = test['trx'].tolist()
  Dict['sales'] = test['sales'].tolist()
  Dict['openrice_name'] = test['openrice_name'].tolist()
  arr.append(Dict)
arr

# COMMAND ----------

arr[-1]

# COMMAND ----------

len(set(arr[-1]['openrice_name']))

# COMMAND ----------

res = []
for i in arr:
  member = len(set(i['member']))
  sales = sum((i['sales']))
  trx = sum((i['trx']))
  rest = len(set(i['openrice_name']))
  res.append((member, sales, trx, rest))

# COMMAND ----------

res[1]

# COMMAND ----------

res_df = pd.DataFrame(res, columns =['member', 'sales', 'trx', 'restaurant'])

# COMMAND ----------

filename = "/dbfs/mnt/terence/hase_openrice/result_agg.csv"
res_df.to_csv(filename, index=False, sep=',')

# COMMAND ----------

res = pd.read_csv("/dbfs/mnt/terence/hase_openrice/result_agg.csv")

# COMMAND ----------

res.shape

# COMMAND ----------

data = []
# i = 'hamburger'
for i in cuisine_list:
  no_of_restaurant = merged_df[merged_df[i]==True].count()[i]
  total_trx = merged_df[merged_df[i]==True].sum()['count']
  data.append((i, no_of_restaurant, total_trx))
data

# COMMAND ----------

res = pd.DataFrame(data, columns =['cuisine', 'no_of_restaurants', 'no_of_trx'])

# COMMAND ----------

final = res.sort_values(['no_of_trx', 'no_of_restaurants'], ascending=[False, False]).reset_index()

# COMMAND ----------

filename = "/dbfs/mnt/terence/hase_openrice/result.csv"
final.to_csv(filename, index=False, sep=',')

# COMMAND ----------

test = merged_df.loc[merged_df['outdoor'] == True]

# COMMAND ----------

test

# COMMAND ----------


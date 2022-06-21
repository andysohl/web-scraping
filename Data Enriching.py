# Databricks notebook source
# MAGIC %md
# MAGIC # Data Enrichment
# MAGIC Now that we've tried to match HASE description with Openrice restaurant names, we need to add the restaurant info to the dataset as well.
# MAGIC 
# MAGIC Data to add from Openrice dataset:
# MAGIC 1. Location (District, subdistrict, address)
# MAGIC 2. Cuisine Type

# COMMAND ----------

import pandas as pd
import numpy as np
import re

# COMMAND ----------

# Read original openrice data
openrice_df = pd.read_csv("/dbfs/mnt/rex/openrice/final_openrice_data_20220104.csv", index_col=0)
openrice_df['name'] = openrice_df['name_2'].fillna(openrice_df['name_1'])
openrice_df['english'] = openrice_df['name'].apply(lambda x: 1 if x.isascii() else 0)
openrice_df = openrice_df.drop(openrice_df[openrice_df['english'] == 0].index)
openrice_df = openrice_df.drop(columns=['english'])
openrice_df = openrice_df.drop(columns=['name_1', 'name_2'])
openrice_df = openrice_df[['name', 'cuisine_type', 'district', 'subdistrict', 'address_1', 'lat', 'long']]
openrice_df

# COMMAND ----------

# Read matching result
matching_df = pd.read_csv('/dbfs/mnt/rex/openrice/verified_matches.csv')
# matching_df = matching_df.drop_duplicates(subset='openrice_name')
matching_df

# COMMAND ----------

merged_df = pd.merge(matching_df, openrice_df, left_on='openrice_name', right_on='name', how='inner')
merged_df = merged_df.drop_duplicates(subset=['hase_name','openrice_name'])
merged_df = merged_df.drop(columns=['name'])
merged_df

# COMMAND ----------

merged_df.info()

# COMMAND ----------

merged_df[merged_df['district'].isnull()]

# COMMAND ----------

merged_df

# COMMAND ----------

merged_df.to_csv('/dbfs/mnt/rex/openrice/exact_match_full_set_20220331.csv', index=None)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assign cuisine type
# MAGIC For non-exact match entries, we can only assign cuisine type to the HASE name.
# MAGIC 
# MAGIC To assign cuisine type to HASE name, we need to build a dictionary that contains the rules that determine what cuisine type should be assigned when certain keywords exist in the HASE name.

# COMMAND ----------

# Read HASE names into list
hase_df = pd.read_csv("/dbfs/mnt/rex/openrice/HASE_restaurant_name_code.csv", index_col=None)
hase_df = hase_df.rename(columns={'HASEHK_PT_TRANSFER_MERCHANT_DESCRIPTION': 'name'})

hase_names = list(hase_df['name'].astype(str).unique()) #unique org names from company watch file
hase_names

# COMMAND ----------

# Read the list of HASE names that are already matched
merged_df = pd.read_csv('/dbfs/mnt/rex/openrice/exact_match_full_set_20220331.csv', index_col=None)
validated_hase_names = list(merged_df['hase_name'].astype(str).unique())
validated_hase_names

# COMMAND ----------

print(len(hase_names))
print(len(validated_hase_names))

# COMMAND ----------

# Get the list of HASE names that are not matched yet, and assign cuisine type to this list
hase_names_list = [x for x in hase_names if x not in validated_hase_names]
print(len(hase_names_list))

# COMMAND ----------

# Set up the dictionary outlining the keywords list to check, and the corresponding cuisine type to assign to the item
rule_dict = {'thai': 'thai',
             'sushi': 'japanese',
             'dim sum': 'chinese/yum cha',
             'coffee': 'western/cha chan ting',
             'cafe': 'western/cha chan ting',
             'ramen': 'japanese',
             'bento': 'japanese',
             'dumpling': 'chinese/dumpling',
             'canteen': 'hong kong style/cha chan ting',
             'bubble tea': 'taiwanese/drinks',
             'boba': 'taiwanese/drinks',
             'shanghai': 'shanghainese',
             'shang hai': 'shanghainese',
             'viet': 'vietnamese',
             'vietnam': 'vietnamese',
             'saigon': 'vietnamese',
             'gyoza': 'japanese',
             'pho': 'vietnamese',
             'sichuan': 'chinese/sichuan',
             'si chuan': 'chinese/sichuan',
             'canton': 'chinese/cantonese',
             'cantonese': 'chinese/cantonese',
             'taipei': 'taiwanese',
             'yakitori': 'japanese/bbq',
             'pho': 'vietnamese',
             'izakaya': 'japanese/bbq/bar',
             'penang': 'singaporean/malaysian',
             'steak': 'western/steak',
             'steakhouse': 'western/steak',
             'burger': 'western/burger',
             'pizza': 'western/italian/pizza',
             'mexico': 'mexican',
             'turkey': 'turkish',
             'mcdonald\'s': 'american/hamburger/fast food',
             'starbucks': 'american/coffee shop/salad/cake',
             'cafe de coral': 'hong kong style/fast food',
             'mx': 'hong kong style/fast food',
             'new york': 'american',
             'ny': 'american',
             'chicago': 'american',
             'los angeles': 'american',
             'san francisco': 'american',
             'seoul': 'korean',
             'busan': 'korean',
             'tokyo': 'japanese',
             'osaka': 'japanese',
             'kyoto': 'japanese',
             'satay': 'singaporean/malaysian',
             'omakase': 'japanese',
             'bangkok': 'thai',
             'paris': 'french',
             'madrid': 'spanish',
             'barcelona': 'spanish',
             'berlin': 'german',
             'frankfurt': 'german',
             'munich': 'german',
             'munchen': 'german',
             'milan': 'italian',
             'rome': 'italian',
             'ristorante': 'italian',
             'pasta': 'italian',
             'spaghetti': 'italian'}

# COMMAND ----------

# Read Cuisine ID
cuisine_type_df = pd.read_csv("/dbfs/mnt/rex/openrice/cuisine_id_list_0406.csv", index_col=0)
cuisine_type_df = cuisine_type_df.rename(columns={' country': 'country'})
cuisine_type_df

# COMMAND ----------

cuisine_type_df.info()

# COMMAND ----------

cuisine_dict = {}
for index, row in cuisine_type_df.iterrows():
    cuisine_dict[row['country'].lower()] = row['eng_name'].lower()
#     cuisine_dict[row['country']] = row['country']
cuisine_dict.pop(' ')
cuisine_dict

# COMMAND ----------

final_dict = {**rule_dict, **cuisine_dict}
final_dict

# COMMAND ----------

len(final_dict)

# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover

extra_stop_word_list = ['foodpanda', 'foodpanda hong kong', 'deliveroo', 'alipayhk*', 'alipayhk *', 'alipay *', 'alipay*', 'eftpay*', 'yedpay*', 'wechat pay hong kong ltd', 'limited', 'ltd', 'shop', 'bar', 'house', 'square']
stop_word_list = list(set(StopWordsRemover().getStopWords()))
foreign_payment_list = ['sq*', 'doordash*', 'paypal*', 'tst*', 'ztl*', 'iz*', 'ghl*', 'crv*', 'iz *', 'sq *', 'paypal *']

def clean(string):
    """Takes an input string, cleans it. 
    """
    string = str(string)
    string = string.lower() # lower case
#     string = fix_text(string) # fix text
    string = string.encode("ascii", errors="ignore").decode() #remove non ascii chars
    chars_to_remove = [")","(",".","|","[","]","{","}","'","-",",","?","!","@","*"]
    rx = '[' + re.escape(''.join(chars_to_remove)) + ']' #remove punc, brackets etc...
    string = re.sub(rx, '', string)
#     string = string.replace('&', 'and')
    for word in stop_word_list:
        rx = r'\b'+word+r'\b'
        string = re.sub(rx, '', string)
    for word in extra_stop_word_list:
        string = string.replace(word, '')
    string = re.sub(' +',' ',string).strip() # get rid of multiple spaces and replace with a single
    for prefix in foreign_payment_list:
        if prefix in string:
            string = ""
            return string
    return string

test_string = 'ALIPAY*MCDONALD\'S'
print('Testing String:')
print('Before: ' + test_string)
print('After: ' + clean(test_string))

# COMMAND ----------

count = 0
matched_df = pd.DataFrame(columns=['hase_name', 'cuisine_type'])
for hase_name in hase_names_list:
    clean_hase_name = clean(hase_name)
#     print(clean_hase_name)
    for key, value in final_dict.items():
#         print(key, value)
        rx = r'\b'+key+r'\b'
        if bool(re.search(rx, clean_hase_name)):
            print("Matched: " + hase_name)
            print("Matched Keyword: " + key)
            print("Assigned cuisine type: " + final_dict[key])
            tmp_dict = {'hase_name': hase_name, 'cuisine_type': final_dict[key]}
            matched_df = matched_df.append(tmp_dict, ignore_index=True)
            count += 1
            break
#         break
#     break
    

# COMMAND ----------

# Final dict
print(count)

# COMMAND ----------

print(matched_df.shape)

# COMMAND ----------

matched_df

# COMMAND ----------

matched_df.to_csv('/dbfs/mnt/rex/openrice/rule_based_match_result_0407.csv', index=None)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calculate the transaction count coverage of the matched entries

# COMMAND ----------

matched_df = pd.read_csv('/dbfs/mnt/rex/openrice/rule_based_match_result_0407.csv', index_col=None)
matched_df

# COMMAND ----------

merged_df = pd.read_csv('/dbfs/mnt/rex/openrice/exact_match_full_set_20220331.csv', index_col=None)
merged_df

# COMMAND ----------

freq_df = pd.read_csv('/dbfs/mnt/rex/openrice/HASE_name_occurences_0406.csv', index_col=None)
freq_df

# COMMAND ----------

freq_df['num_txn'].sum()

# COMMAND ----------

print(freq_df[freq_df['num_txn'] > 1000]['num_txn'].sum() / freq_df['num_txn'].sum())
print(freq_df[freq_df['num_txn'] > 1000]['num_txn'].sum() / freq_df['num_txn'].sum() * freq_df['num_txn'].sum())

# COMMAND ----------

freq_df['matched'] = 0
freq_df

# COMMAND ----------

exact_match = merged_df['hase_name'].tolist()
approx_match = matched_df['hase_name'].tolist()
verified = exact_match + approx_match
len(verified)

# COMMAND ----------

for index, row in freq_df.iterrows():
    hase_name = row['HASEHK_PT_TRANSFER_MERCHANT_DESCRIPTION']
    for item in verified:
        if hase_name == item:
            print('match found')
            freq_df.at[index, 'matched'] = 1
            break
#     break
freq_df['matched'].value_counts()

# COMMAND ----------

freq_df['matched'].value_counts()

# COMMAND ----------

print("Coverage: " + str(freq_df[freq_df['matched'] == 1]['num_txn'].sum() / freq_df['num_txn'].sum()))
print("Total txn count matched: " + str(freq_df[freq_df['matched'] == 1]['num_txn'].sum()))
print("Total txn count matched: " + str(freq_df['num_txn'].sum()))
print("Total txn count up to April 6, 2022")

# COMMAND ----------

merged_df

# COMMAND ----------

matched_df['openrice_name'] = 'NA'
matched_df['district'] = 'NA'
matched_df['subdistrict'] = 'NA'
matched_df['address_1'] = 'NA'
matched_df['lat'] = 0
matched_df['long'] = 0
matched_df = matched_df[['hase_name', 'openrice_name', 'cuisine_type', 'district', 'subdistrict', 'address_1', 'lat', 'long']]
matched_df

# COMMAND ----------

final_df = merged_df.append(matched_df)
final_df

# COMMAND ----------

final_df['cuisine_type'] = final_df['cuisine_type'].apply(lambda x: x.lower())
final_df = final_df.rename(columns={'address_1': 'address'})
final_df

# COMMAND ----------

final_df.to_csv('/dbfs/mnt/rex/openrice/PUBLISH_YUU.OPENRICE_MATCHED_CUISINE_20220408.csv', index=None)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest into DB

# COMMAND ----------

# MAGIC %run /Users/trncetou@dairy-farm.com.hk/helper_cls

# COMMAND ----------

# MAGIC %run /Users/trncetou@dairy-farm.com.hk/helper_p_cls

# COMMAND ----------

helper1 = Helper_p()  # For Reading live data from Production 
helper2 = Helper()  # For Writng data into different environment 

# COMMAND ----------

import pyspark
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

sdf = spark.createDataFrame(final_df)
print(sdf)

# COMMAND ----------

print((sdf.count(), len(sdf.columns)))

# COMMAND ----------

sdf.take(1)

# COMMAND ----------

table = 'PUBLISH_YUU.OPENRICE_MATCHED_CUISINE_20220408'
helper1.write_to_sqldw(sdf, table, 'OVERWRITE') # When creating a new table
# helper1.write_to_sqldw(sdf, table, 'APPEND') # When appending new rows to an existing table

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get all cuisine types available

# COMMAND ----------

cuisine_type_list = final_df['cuisine_type'].tolist()
cuisine_type_list

# COMMAND ----------

cuisine_list = []
for item in cuisine_type_list:
    tmp_list = item.split('/')
    for cuisine in tmp_list:
        cuisine_list.append(cuisine)
print(len(cuisine_list))

# COMMAND ----------

final_cuisine_list = list(set(cuisine_list))
final_cuisine_list

# COMMAND ----------

len(final_cuisine_list)

# COMMAND ----------

import csv

with open('/dbfs/mnt/rex/openrice/cuisine_type_list.csv', 'w') as f:
      
    # using csv.writer method from CSV package
    write = csv.writer(f)
    
    for item in final_cuisine_list:
        write.writerow([item])
    

# COMMAND ----------


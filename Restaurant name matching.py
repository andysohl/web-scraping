# Databricks notebook source
# MAGIC %md
# MAGIC ## Restaurant Name Matching
# MAGIC 
# MAGIC OpenRice Data Scraping Follow-up
# MAGIC 
# MAGIC Trying to match the restaurant info to the HASE transaction entries in DB. Might try using this Python library: thefuzz (https://github.com/seatgeek/thefuzz)

# COMMAND ----------

import pandas as pd
import numpy as np
from thefuzz import fuzz
from thefuzz import process
import csv
import os

# COMMAND ----------

df = pd.read_csv('/dbfs/mnt/rex/openrice/HASE_restaurant_name_code.csv')
df

# COMMAND ----------

openrice_df = pd.read_csv('/dbfs/mnt/rex/openrice/final_openrice_data_wo_polygon.csv')
openrice_df

# COMMAND ----------

openrice_df[openrice_df['name_2'] == 'Hainan Shaoye']

# COMMAND ----------

df['HASEHK_PT_TRANSFER_MERCHANT_DESCRIPTION'].drop_duplicates()

# COMMAND ----------

hase_name = df['HASEHK_PT_TRANSFER_MERCHANT_DESCRIPTION'].tolist()
openrice_name1 = openrice_df['name_1'].tolist()
openrice_name2 = openrice_df['name_2'].tolist()
print(len(hase_name))
print(len(openrice_name1))
print(len(openrice_name2))

# COMMAND ----------

pd.isna(openrice_name2[0])

# COMMAND ----------

rows = []
fields = ['HASE_name', 'OPENRICE_name', 'SIM_SCORE']
k = 0
for i in range(1, len(hase_name)+1):
    best_score = 0
    best_pair = (hase_name[i], openrice_name1[0])
    for j in range(len(openrice_name1)):
        if pd.isna(openrice_name2[j]):
            openrice_name = openrice_name1[j]
        else:
            openrice_name = openrice_name2[j]
        score = fuzz.token_sort_ratio(hase_name[i], openrice_name)
        if score >= best_score:
            best_score = score
            best_pair = (hase_name[i], openrice_name)
#         print("Parital Ratio: {}".format(fuzz.partial_ratio(hase_name[i], openrice_name)))
#         print("Token Sort Ratio: {}".format(fuzz.token_sort_ratio(hase_name[i], openrice_name)))
#         print("Token Set Ratio: {}".format(fuzz.token_set_ratio(hase_name[i], openrice_name)))
#         break
    if best_score == 100:  
        print("Best match with '{}' is '{}' with the ratio score of {}".format(best_pair[0], best_pair[1], best_score))
        rows.append([best_pair[0], best_pair[1], best_score])
#         print(len(pairs_list))
#     else:
#         print("No good match with '{}'".format(hase_name[i]))
    if len(rows) % 100 == 0 and len(rows) > 0:
        k += 1
        filename = "/dbfs/mnt/rex/rest_name_match_part{}.csv".format(k)
        with open(filename, 'w') as f:
            # using csv.writer method from CSV package
            write = csv.writer(f)
            write.writerow(fields)
            write.writerows(rows)
        rows = []
        print("100 rows has been saved to {}".format(filename))
    if i == len(hase_name):
        k += 1
        filename = "/dbfs/mnt/rex/rest_name_match_part{}.csv".format(k)
        with open(filename, 'w') as f:
            # using csv.writer method from CSV package
            write = csv.writer(f)
            write.writerow(fields)
            write.writerows(rows)
        rows = []

# COMMAND ----------

df = pd.read_csv("/dbfs/mnt/rex/openrice/rest_name_match/rest_name_match_part1.csv")
df

# COMMAND ----------

file_list = os.listdir("/dbfs/mnt/rex/openrice/rest_name_match/")
file_list

# COMMAND ----------

df = pd.DataFrame()
for file in file_list:
    tmp_df = pd.read_csv("/dbfs/mnt/rex/openrice/rest_name_match/" + file)
    df = pd.concat([df, tmp_df])
    
df

# COMMAND ----------

df = df.drop_duplicates()
df

# COMMAND ----------


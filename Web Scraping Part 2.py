# Databricks notebook source
# MAGIC %md
# MAGIC # OpenRice Web Scraping Part 2
# MAGIC Scrape price range from the OpenRice website for each restautant.
# MAGIC 
# MAGIC Just matched restautant is OK for now.

# COMMAND ----------

pip install bs4

# COMMAND ----------

import pandas as pd
import json
import urllib
from bs4 import BeautifulSoup
import csv

# COMMAND ----------

openrice_df = pd.read_csv("/dbfs/mnt/rex/openrice/final_openrice_data_wo_polygon.csv", index_col=None)
openrice_df = openrice_df[['name_1', 'name_2', 'url']]
openrice_df['name'] = openrice_df['name_2'].fillna(openrice_df['name_1'])
openrice_df = openrice_df.drop(columns=['name_1', 'name_2'])
openrice_df = openrice_df.drop_duplicates()
openrice_df['english'] = openrice_df['name'].apply(lambda x: 1 if x.isascii() else 0)
openrice_df = openrice_df.drop(openrice_df[openrice_df['english'] == 0].index)
openrice_df = openrice_df.drop(columns=['english'])
openrice_df = openrice_df[['name', 'url']]
openrice_df

# COMMAND ----------

openrice_df = openrice_df.drop_duplicates(subset="name")
openrice_df

# COMMAND ----------

db_df = pd.read_csv("/dbfs/mnt/rex/openrice/PUBLISH_YUU.OPENRICE_MATCHED_CUISINE_20220408.csv", index_col=None)
db_df

# COMMAND ----------

merged_df = pd.merge(db_df, openrice_df, left_on="openrice_name", right_on="name", how="left")
merged_df

# COMMAND ----------

url_df = merged_df[['openrice_name', 'url']]
url_df['price_range'] = ""
url_df

# COMMAND ----------

poi_url_list = url_df.url.tolist()
i = 0
prev_url = ""
prev_price = ""
for url in poi_url_list:
    if (url == prev_url) & (i > 0):
        print("Same URL as last item")
        url_df.iloc[i, -1] = prev_price
        i += 1
    else:
        print("Accessing " + url)
        request = urllib.request.Request(url)
        request.add_header('User-Agent', 'Mozilla/5.0')
        try:
            response = urllib.request.urlopen(request)
        except urllib.error.HTTPError as exc:
            print("Cannot access " + url)
    #         time.sleep(10) # wait 10 seconds and then make http request again
            continue
        try:
            html = response.read()
        except Exception as e:
            html = e.partial
        soup = BeautifulSoup(html, 'html.parser')
        price_div = soup.find('div', {'class': 'header-poi-price dot-separator'})
        price_txt = price_div.text
        url_df.iloc[i, -1] = price_txt.strip()
        i += 1
        prev_url = url
        prev_price = price_txt.strip()
#     if i == 5:
#         break

# COMMAND ----------

url_df.head(10)

# COMMAND ----------

url_df['price_range'].value_counts()

# COMMAND ----------

url_df.loc[url_df['price_range'] == "Below $50", 'price_range'] = "0-50"
url_df.loc[url_df['price_range'] == "$51-100", 'price_range'] = "51-100"
url_df.loc[url_df['price_range'] == "$101-200", 'price_range'] = "101-200"
url_df.loc[url_df['price_range'] == "$201-400", 'price_range'] = "201-400"
url_df.loc[url_df['price_range'] == "$401-800", 'price_range'] = "401-800"
url_df.loc[url_df['price_range'] == "Above $801", 'price_range'] = "801+"
url_df

# COMMAND ----------

merged_df['price_range'] = url_df['price_range']
merged_df

# COMMAND ----------

merged_df = merged_df[['hase_name', 'openrice_name', 'cuisine_type', 'district', 'subdistrict', 'address', 'lat', 'long', 'price_range']]
merged_df

# COMMAND ----------

merged_df.to_csv("/dbfs/mnt/rex/openrice/PUBLISH_YUU.OPENRICE_MATCHED_CUISINE_20220503.csv")

# COMMAND ----------

merged_df = pd.read_csv("/dbfs/mnt/rex/openrice/PUBLISH_YUU.OPENRICE_MATCHED_CUISINE_20220503.csv", index_col=0)
merged_df

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

sdf = spark.createDataFrame(merged_df)
print(sdf)

# COMMAND ----------

print((sdf.count(), len(sdf.columns)))

# COMMAND ----------

sdf.take(1)

# COMMAND ----------

table = 'PUBLISH_YUU.OPENRICE_MATCHED_CUISINE_20220503'
helper1.write_to_sqldw(sdf, table, 'OVERWRITE') # When creating a new table
# helper1.write_to_sqldw(sdf, table, 'APPEND') # When appending new rows to an existing table

# COMMAND ----------


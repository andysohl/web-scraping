# Databricks notebook source
# MAGIC %md
# MAGIC # HASE Description Matching (Spark implementation)
# MAGIC Trying to match Openrice data with HASE transaction penetration
# MAGIC 
# MAGIC Inspired by this article (https://medium.com/analytics-vidhya/fuzzy-string-matching-with-spark-in-python-7fcd0c422f71)

# COMMAND ----------

import pandas as pd
import numpy as np
from thefuzz import fuzz
from thefuzz import process
import csv
import os
import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import monotonically_increasing_id, regexp_replace

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Remove restaurants that only has Chinese names

# COMMAND ----------

openrice_df = pd.read_csv('/dbfs/mnt/rex/openrice/final_openrice_data_wo_polygon.csv')
openrice_df = openrice_df[['name_1', 'name_2']]
openrice_df['name'] = openrice_df['name_2'].fillna(openrice_df['name_1'])
openrice_df = openrice_df.drop(columns=['name_1', 'name_2'])
openrice_df = openrice_df.drop_duplicates()
openrice_df['english'] = openrice_df['name'].apply(lambda x: 1 if x.isascii() else 0)
openrice_df = openrice_df.drop(openrice_df[openrice_df['english'] == 0].index)
openrice_df = openrice_df.drop(columns=['english'])
openrice_df

# COMMAND ----------

openrice_df.to_csv("/dbfs/mnt/rex/openrice/openrice_restaurant_names.csv", index=None)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read Data as spark dataframe

# COMMAND ----------

hase_ddf = (spark.read.csv('/mnt/rex/openrice/HASE_restaurant_name_code.csv', header=True, inferSchema=True)
              .select(F.col('HASEHK_PT_TRANSFER_MERCHANT_DESCRIPTION').alias('name')))
hase_ddf = hase_ddf.distinct()
hase_ddf = hase_ddf.withColumn("name", regexp_replace(F.col('name'), "/[&]/", "and"))
hase_ddf = hase_ddf.select("*").withColumn("hase_id", monotonically_increasing_id())
hase_ddf.select('hase_id','name').show(5, False)
print(f"{hase_ddf.count()} HASE descriptions")

# COMMAND ----------

openrice_ddf = (spark.read.csv('/mnt/rex/openrice/openrice_restaurant_names.csv', header=True, inferSchema=True))
openrice_ddf = openrice_ddf.distinct()
openrice_ddf = openrice_ddf.withColumn("name", regexp_replace(F.col('name'), "/[&]/", "and"))
openrice_ddf = openrice_ddf.select("*").withColumn("openrice_id", monotonically_increasing_id())
openrice_ddf.select('openrice_id','name').show(5, False)
print(f"{openrice_ddf.count()} openrice names")

# COMMAND ----------

# MAGIC %md
# MAGIC #### When joining exact matching names

# COMMAND ----------

result = openrice_ddf.join(hase_ddf, 'name')
result.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Record Linkage (Fuzzy Matching)
# MAGIC #### Prepare join column by doing multiple transformations
# MAGIC 1. Convert all characters to lower case
# MAGIC 2. Tokenize the string (split string by whitespace)
# MAGIC 3. Remove stop words (e.g. the, a, an, ...)
# MAGIC 4. Convert strings into n-grams (2-grams, 3-grams, i.e. 2 char combinations from the whole string)
# MAGIC 5. Vectorize n-grams (convert into numeric values)
# MAGIC 6. MinHash the vectors (faster computation of distances)
# MAGIC 
# MAGIC See output for interim products in the pipeline.

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StopWordsRemover, Tokenizer, NGram, HashingTF, MinHashLSH, RegexTokenizer, SQLTransformer

stop_word_list = ['restaurant', 'cafe', 'coffee', 'banquet', 'seafood', 'kitchen', 'foodpanda', 'foodpanda hong kong', 'deliveroo', 'alipayhk', 'alipay', 'sq', 'eftpay', 'yedpay', 'wechat pay hong kong ltd', '*', '[', ']', "'", ',', '.', '(', ')', '-']
stop_word_list.extend(StopWordsRemover().getStopWords())
stop_word_list = list(set(stop_word_list))

# Create preprocessing pipeline for HASE descriptions
model = Pipeline(stages=[
    SQLTransformer(statement="SELECT *, lower(name) lower FROM __THIS__"),
    Tokenizer(inputCol="lower", outputCol="token"),
    StopWordsRemover(inputCol="token", outputCol="stop", stopWords=stop_word_list),
    SQLTransformer(statement="SELECT *, concat_ws(' ', stop) concat FROM __THIS__"),
    RegexTokenizer(pattern="", inputCol="concat", outputCol="char", minTokenLength=1),
    NGram(n=2, inputCol="char", outputCol="ngram"),
    HashingTF(inputCol="ngram", outputCol="vector"),
    MinHashLSH(inputCol="vector", outputCol="lsh", numHashTables=3)
]).fit(hase_ddf)

result_hase = model.transform(hase_ddf)
result_hase = result_hase.filter(F.size(F.col("ngram")) > 0)
print(f"Example transformation ({result_hase.count()} HASE names left):")
result_hase.select('hase_id', 'name', 'concat', 'char', 'ngram', 'vector', 'lsh').show(1)

# COMMAND ----------

# Use the above defined pipeline for openrice names as well
# Filtered entries are names all comprised of stopwords
result_openrice = model.transform(openrice_ddf)
filtered = result_openrice.filter(F.size(F.col("ngram")) < 1)
print(f"Filtered out rows: {filtered.count()}")
filtered.select('name', 'concat', 'char', 'ngram', 'vector').show()
result_openrice = result_openrice.filter(F.size(F.col("ngram")) > 0)
print(f"Example transformation ({result_openrice.count()} openrice names left):")
result_openrice.select('openrice_id', 'name', 'concat', 'char', 'ngram', 'vector', 'lsh').show(1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join based on Jaccard Distance
# MAGIC Join both datasets by supplying a maximum Jaccard Distance threshold that result in a match. Lowering the threshold will provide more exact matching and thus creating fewer false positives but could potentially lead to missing some true positives. A Jaccard Distance of 0.5 is the equivalent of a two-thirds match if both strings are of equal length

# COMMAND ----------

result = model.stages[-1].approxSimilarityJoin(result_openrice, result_hase, 0.5, "jaccardDist")
print(f"{result.count()} matches")
(result
 .select('datasetA.openrice_id', 'datasetA.name', 'datasetB.name', 'jaccardDist')
 .sort(F.col('datasetA.openrice_id'))
 .show(5, True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimization: Only get the one with closest distance
# MAGIC Instead of lowering the Jaccard Distance to have fewer but more correct matches, you could select the match with the minimum distance at an extra computational cost. This is done by aggregating over id.

# COMMAND ----------

from pyspark.sql import Window
w = Window.partitionBy('datasetA.openrice_id')
result = (result
           .withColumn('minDist', F.min('jaccardDist').over(w))
           .where(F.col('jaccardDist') == F.col('minDist'))
           .drop('minDist'))
print(f"{result.count()} matches")
(result
 .select('datasetA.name', 'datasetB.name', 'jaccardDist')
 .sort(F.col('datasetA.openrice_id'))
 .show(5))

# COMMAND ----------

result = (result
 .select('datasetA.name', 'datasetB.name', 'jaccardDist')
 .sort(F.col('datasetA.openrice_id')))

# COMMAND ----------

# Convert spark DF to pandas DF
result_df = result.toPandas()
result_df

# COMMAND ----------

result_df[(result_df['jaccardDist'] >= 0.0) & (result_df['jaccardDist'] < 0.1)]

# COMMAND ----------

result_df.to_csv("/dbfs/mnt/rex/openrice/matching_result_20210304.csv", index=None)

# COMMAND ----------


# Databricks notebook source
# MAGIC %md
# MAGIC # Restaurant Name Deduping
# MAGIC Both list of names from the HASE description and Openrice Data are a bit messy, this notebook aims to find close matching to deduplicate entries, hence cleaning up the name list.
# MAGIC 
# MAGIC Copied from this article: https://towardsdatascience.com/fuzzy-matching-at-scale-84f2bfd0c536

# COMMAND ----------

pip install ftfy #  text cleaning for decode issues..

# COMMAND ----------

pip install --upgrade numpy

# COMMAND ----------

pip install nmslib

# COMMAND ----------

import pandas as pd
pd.set_option('display.max_colwidth', None)
from tqdm import tqdm
import os
import numpy as np
import os
import pickle #optional - for saving outputs
import re
from tqdm import tqdm # used for progress bars (optional)
import time
from ftfy import fix_text
from sklearn.feature_extraction.text import TfidfVectorizer
import time
import nmslib
from scipy.sparse import csr_matrix # may not be required 
from scipy.sparse import rand # may not be required

# COMMAND ----------

stop_word_list = ['restaurant', 'cafe', 'coffee', 'banquet', 'seafood', 'kitchen', 'foodpanda', 'foodpanda hong kong', 'deliveroo', 'alipayhk', 'alipay', 'sq', 'eftpay', 'yedpay', 'wechat pay hong kong ltd']

def ngrams(string, n=3):
    """Takes an input string, cleans it and converts to ngrams. 
    This script is focussed on cleaning UK company names but can be made generic by removing lines below"""
    string = str(string)
    string = string.lower() # lower case
    string = fix_text(string) # fix text
    string = string.encode("ascii", errors="ignore").decode() #remove non ascii chars
    chars_to_remove = [")","(",".","|","[","]","{","}","'","-","*"]
    rx = '[' + re.escape(''.join(chars_to_remove)) + ']' #remove punc, brackets etc...
    string = re.sub(rx, '', string)
    string = string.replace('&', 'and')
    for word in stop_word_list:
        string = string.replace(word, '')
    string = re.sub(' +',' ',string).strip() # get rid of multiple spaces and replace with a single
    string = ' '+ string +' ' # pad names for ngrams...
    ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams]

print('All 3-grams in "Department":')
print(ngrams('Depar-tment &, Ltd'))

# COMMAND ----------

# Load HASE names and build TF-IDF matrix of the list
t1 = time.time() # used for timing - can delete
hase_df = pd.read_csv("/dbfs/mnt/rex/openrice/HASE_restaurant_name_code.csv", index_col=None)
hase_df = hase_df.rename(columns={'HASEHK_PT_TRANSFER_MERCHANT_DESCRIPTION': 'name'})

hase_names = list(hase_df['name'].astype(str).unique()) #unique org names from company watch file

#Building the TFIDF off the clean dataset
vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
tf_idf_matrix = vectorizer.fit_transform(hase_names)
t = time.time()-t1
print("Time:", t) # used for timing - can delete
print(tf_idf_matrix.shape)

# COMMAND ----------

# Load openrice names and build TF-IDF matrix of the list
t1 = time.time() # used for timing - can delete
openrice_df = pd.read_csv("/dbfs/mnt/rex/openrice/openrice_restaurant_names.csv", index_col=None)
# openrice_df = openrice_df.rename(columns={'HASEHK_PT_TRANSFER_MERCHANT_DESCRIPTION': 'name'})

openrice_names = list(openrice_df['name'].unique()) #unique org names from company watch file

#Building the TFIDF off the clean dataset
# vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
openrice_tf_idf_matrix = vectorizer.transform(openrice_names)
t = time.time()-t1
print("Time:", t) # used for timing - can delete
print(openrice_tf_idf_matrix.shape)

# COMMAND ----------

# create a random matrix to index
data_matrix = tf_idf_matrix#[0:1000000]

# Set index parameters
# These are the most important ones
M = 80
efC = 1000

num_threads = 4 # adjust for the number of threads
# Intitialize the library, specify the space, the type of the vector and add data points 
index = nmslib.init(method='simple_invindx', space='negdotprod_sparse_fast', data_type=nmslib.DataType.SPARSE_VECTOR) 

index.addDataPointBatch(data_matrix)
# Create an index
start = time.time()
index.createIndex() 
end = time.time() 
print('Indexing time = %f' % (end-start))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try with more neighbors

# COMMAND ----------

num_threads = 4
K = 1 # Number of neighbors 
query_matrix = openrice_tf_idf_matrix
start = time.time() 
query_qty = query_matrix.shape[0]
nbrs = index.knnQueryBatch(query_matrix, k = K, num_threads = num_threads)
end = time.time() 
print('kNN time total=%f (sec), per query=%f (sec), per query adjusted for thread number=%f (sec)' % 
      (end-start, float(end-start)/query_qty, num_threads*float(end-start)/query_qty))

# COMMAND ----------

mts =[]
for i in range(len(nbrs)):
  original_nm = openrice_names[i]
  try:
    matched_nm   = hase_names[nbrs[i][0][0]]
    conf         = nbrs[i][1][0]
  except:
    matched_nm   = "no match found"
    conf         = None
  mts.append([original_nm,matched_nm,conf])

mts = pd.DataFrame(mts,columns=['original_name','matched_name','conf'])
results = openrice_df.merge(mts,left_on='name', right_on='original_name')

results.conf.hist()
#Profile of matches - lower is higher confidence

# COMMAND ----------

mts

# COMMAND ----------

mts['conf'] = mts['conf'].abs()
mts

# COMMAND ----------

mts[mts['matched_name'] == "no match found"]

# COMMAND ----------

mts[(mts['conf'] >= 0.5) & (mts['conf'] <= 0.6)]

# COMMAND ----------

mts.to_csv("/dbfs/mnt/rex/openrice/matching_result_20220304_nms.csv", index=None)

# COMMAND ----------


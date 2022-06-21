# Databricks notebook source
pip install --upgrade pip

# COMMAND ----------

pip install polyfuzz[flair]

# COMMAND ----------

import pandas as pd
import re
from polyfuzz import PolyFuzz
from polyfuzz.models import EditDistance, TFIDF, Embeddings, RapidFuzz

# COMMAND ----------

# Read HASE names into list
hase_df = pd.read_csv("/dbfs/mnt/rex/openrice/HASE_restaurant_name_code.csv", index_col=None)
hase_df = hase_df.rename(columns={'HASEHK_PT_TRANSFER_MERCHANT_DESCRIPTION': 'name'})

hase_names = list(hase_df['name'].astype(str).unique()) #unique org names from company watch file
hase_names

# COMMAND ----------

# Read openrice names
openrice_df = pd.read_csv("/dbfs/mnt/rex/openrice/openrice_restaurant_names.csv", index_col=None)
openrice_names = list(openrice_df['name'].unique())
openrice_names

# COMMAND ----------

print(len(hase_names))
print(len(openrice_names))

# COMMAND ----------

from pyspark.ml.feature import StopWordsRemover
extra_stop_word_list = ['restaurant', 'cafe', 'coffee', 'banquet', 'seafood', 'kitchen', 'cuisine', 'bakery', 'foodpanda', 'foodpanda hong kong', 'deliveroo', 'alipayhk*', 'alipayhk *', 'alipay *', 'alipay*', 'eftpay*', 'yedpay*', 'wechat pay hong kong ltd', 'limited', 'ltd', 'bbq', 'barbeque', 'sushi', 'shop', 'bar', 'fusion', 'house', 'square']
# stop_word_list.extend(StopWordsRemover().getStopWords())
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
    string = string.replace('&', 'and')
    for word in stop_word_list:
        rx = r'\b'+word+'\b'
        string = re.sub(rx, '', string)
    for word in extra_stop_word_list:
        string = string.replace(word, '')
    string = re.sub(' +',' ',string).strip() # get rid of multiple spaces and replace with a single
    for prefix in foreign_payment_list:
        if prefix in string:
            string = ""
            return string
    return string

test_string = 'foodpanda*Mos Burger ()'
print('Testing String:')
print('Before: ' + test_string)
print('After: ' + clean(test_string))

# COMMAND ----------

hase_index_df = pd.DataFrame(pd.unique(hase_df['name'])).reset_index()
hase_index_df = hase_index_df.rename(columns={'index': 'hase_index', 0: 'hase_name'})
hase_index_df['clean_hase_name'] = hase_index_df['hase_name'].apply(lambda x: clean(x))
hase_index_df = hase_index_df.drop(hase_index_df[hase_index_df['clean_hase_name'].apply(len) < 5].index)
hase_index_df

# COMMAND ----------

openrice_index_df = pd.DataFrame(pd.unique(openrice_df['name'])).reset_index()
openrice_index_df = openrice_index_df.rename(columns={'index': 'openrice_index', 0: 'openrice_name'})
openrice_index_df['clean_openrice_name'] = openrice_index_df['openrice_name'].apply(lambda x: clean(x))
openrice_index_df = openrice_index_df.drop(openrice_index_df[openrice_index_df['clean_openrice_name'].map(len) < 5].index)
openrice_index_df

# COMMAND ----------

clean_openrice_names = openrice_index_df['clean_openrice_name'].tolist()
clean_hase_names = hase_index_df['clean_hase_name'].tolist()
print("Openrice List Length: " + str(len(clean_openrice_names)))
print("HASE List Length: " + str(len(clean_hase_names)))

# COMMAND ----------

# Use TF-IDF as similarity measure
tfidf_matcher = TFIDF(n_gram_range=(3, 3), min_similarity=0, model_id="TF-IDF")
model = PolyFuzz(tfidf_matcher).match(clean_hase_names, clean_openrice_names)
model.get_matches()

# COMMAND ----------

display(model.visualize_precision_recall(kde=True))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Group Matches
# MAGIC Group matches by setting a threshold on similarity score

# COMMAND ----------

model.group(link_min_similarity=0.9)
result_df = model.get_matches()
result_df

# COMMAND ----------

result_df[result_df['Similarity'] > 0.9]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multiple Models
# MAGIC Compute matching using multiple models and then compare between them.

# COMMAND ----------

from polyfuzz.models import EditDistance, TFIDF, Embeddings, RapidFuzz
from polyfuzz import PolyFuzz

# from jellyfish import jaro_winkler_similarity
from flair.embeddings import WordEmbeddings
# TransformerWordEmbeddings, 

# BERT (not using, might need GPU)
# bert = TransformerWordEmbeddings('bert-base-multilingual-cased')  # https://huggingface.co/transformers/pretrained_models.html
# bert_matcher = Embeddings(bert, model_id="BERT", min_similarity=0)

# FastText
fasttext = WordEmbeddings('en-crawl')
fasttext_matcher = Embeddings(fasttext, min_similarity=0)

# TF-IDF
tfidf_matcher = TFIDF(n_gram_range=(3, 3), min_similarity=0, model_id="TF-IDF")
tfidf_large_matcher = TFIDF(n_gram_range=(3, 6), min_similarity=0)

# Edit Distance models with custom distance function
base_edit_matcher = EditDistance(n_jobs=-1)
# jellyfish_matcher = EditDistance(n_jobs=1, scorer=jaro_winkler_similarity)

# Edit distance with RapidFuzz --> slightly faster implementation than Edit Distance
rapidfuzz_matcher = RapidFuzz(n_jobs=-1)

# bert_matcher, 
matchers = [fasttext_matcher, tfidf_matcher, tfidf_large_matcher,
            base_edit_matcher, rapidfuzz_matcher]

# COMMAND ----------

model = PolyFuzz(matchers).match(clean_hase_names, clean_openrice_names)

# COMMAND ----------

display(model.visualize_precision_recall(kde=True))

# COMMAND ----------

model.group(link_min_similarity=0.75)
result_df = model.get_matches()
result_df

# COMMAND ----------

model.get_matches("Model 0")

# COMMAND ----------

model_0_df = model.get_matches("Model 0")
model_0_df.to_csv("/dbfs/mnt/rex/openrice/0318/fasttext_result.csv")

# COMMAND ----------

model_0_df = model.get_matches("TF-IDF")
model_0_df.to_csv("/dbfs/mnt/rex/openrice/0318/TF-IDF_result.csv")

# COMMAND ----------

model_0_df = model.get_matches("Model 2")
model_0_df.to_csv("/dbfs/mnt/rex/openrice/0318/TF-IDF-large_result.csv")

# COMMAND ----------

model_0_df = model.get_matches("Model 3")
model_0_df.to_csv("/dbfs/mnt/rex/openrice/0318/edit-distance_result.csv")

# COMMAND ----------

model_4_df = model.get_matches("Model 4")
model_4_df

# COMMAND ----------

model_4_df.to_csv("/dbfs/mnt/rex/openrice/0318/rapidfuzz_result.csv")

# COMMAND ----------

model_4_df = pd.read_csv("/dbfs/mnt/rex/openrice/0318/rapidfuzz_result.csv", index_col=0)
model_4_df

# COMMAND ----------

model_4_df = model_4_df.drop(columns=['Group'])
model_4_df = model_4_df.rename(columns={'From': 'clean_hase_name', 'To': 'clean_openrice_name', 'Similarity': 'score'})
model_4_df

# COMMAND ----------

model_4_df[model_4_df['clean_hase_name'] == 'square']

# COMMAND ----------

openrice_index_df

# COMMAND ----------

final_df = model_4_df.merge(hase_index_df.drop(columns=['hase_index']), how='inner')
final_df = final_df.drop_duplicates()
final_df = final_df.merge(openrice_index_df.drop(columns=['openrice_index']), how='inner')
final_df = final_df.drop_duplicates()
final_df

# COMMAND ----------

final_df = final_df[['clean_hase_name', 'hase_name', 'clean_openrice_name', 'openrice_name', 'score']]
final_df

# COMMAND ----------

final_df[final_df['score'] >= 0.8]

# COMMAND ----------

final_df.to_csv("/dbfs/mnt/rex/openrice/final_result_0318.csv")

# COMMAND ----------


# Databricks notebook source
dbutils.widgets.text("Location ID", "")
dbutils.widgets.text("Cuisine ID", "")
dbutils.widgets.text("District Name", "")

# COMMAND ----------

dbutils.widgets.remove("Location ID")
dbutils.widgets.remove("Cuisine ID")
dbutils.widgets.remove("District Name")

# COMMAND ----------

# MAGIC %md
# MAGIC # Restaurant Info Scraping (from OpenRice)
# MAGIC 
# MAGIC Scrape restaurant data from OpenRice website.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Steps
# MAGIC 1. Search restaurants in OpenRice website by applying filters (cuisine type, location, etc.) Due to the fact that the website cannot show more than 17x15=255 restaurants at one time. (Hard restrictions imposed by OpenRice)
# MAGIC 
# MAGIC 2. Retrieve the restaurant page from the search results
# MAGIC 
# MAGIC URL: "https://www.openrice.com/zh/hongkong/restaurants?categoryGroupId=[]&districtId=[]"
# MAGIC 
# MAGIC ##### Link to Restaurant Page in: (Div, find 'a', href=True)
# MAGIC 
# MAGIC 3. Acccess the restaurant pages obtained from step 2 and extract the following data from the page
# MAGIC 
# MAGIC ##### Restaurant Name (Div, class: poi-name)
# MAGIC 
# MAGIC ##### Restaurant District (Div, class: header-poi-district dot-separator)
# MAGIC 
# MAGIC ##### Restaurant Price Range (Div, class: header-poi-price dot-separator)
# MAGIC 
# MAGIC ##### Restaurant Cuisine Type (Div, class: header-poi-categories dot-separator, multiple values)
# MAGIC 
# MAGIC ##### Restaurant Address (Chinese & English) (Div, class: address-info-section)
# MAGIC 
# MAGIC ##### Restaurant Phone Number (Section, class: telephone-section; div, class: content)
# MAGIC 
# MAGIC ##### Restaurant Bookmark Count on OpenRice (Div, class: header-bookmark-count js-header-bookmark-count)
# MAGIC 
# MAGIC ##### Restaurant Page URL

# COMMAND ----------

pip install bs4

# COMMAND ----------

import pandas as pd
import json
import urllib
from bs4 import BeautifulSoup
import csv

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read in Codes for filtering search results
# MAGIC Read the location and cuisine for searching restaurants, from the csv files prepared previously.

# COMMAND ----------

  # Read location ID
location_df = spark.read.csv("/mnt/rex/location_id_list.csv",header=True,inferSchema=True)
display(location_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cuisine ID is split into categoryGroupID (5-digit) & cuisineID (4-digit)

# COMMAND ----------

# Read Cuisine ID
cuisine_type_df = spark.read.csv("/mnt/rex/cuisine_id_list.csv",header=True,inferSchema=True)
display(cuisine_type_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Access Searching page to get URLs to restaurant page
# MAGIC Use the code retrieved from the last step to get restaurant page URL from the search results

# COMMAND ----------

# Search by district filter and cuisine filter to obtain URL to restaurant pages
poi_url_list = []
cuisine_id = dbutils.widgets.get("Cuisine ID")
district_id = dbutils.widgets.get("Location ID")
for i in range(17):
    print("Scraping URLs from page " + str(i+1))
    request = urllib.request.Request("https://www.openrice.com/en/hongkong/restaurants?cuisineId=" + cuisine_id + "&districtId=" + district_id + "&page=" + str(i+1))
    request.add_header('User-Agent', 'Mozilla/5.0')
    response = urllib.request.urlopen(request)
    try:
        html = response.read()
    except:
        print("error happened when reading page " + str(i+1))
        continue
    soup = BeautifulSoup(html, 'html.parser')
    target = soup.findAll('div',{'class':"content-cell-wrapper"})
    for tar in target:
        if tar.find("span", {"class": "pois-restaurant-list-cell-sponsored"}) is None:
            res_url = tar.find("a",href=True)
            full_url = "https://www.openrice.com" + res_url['href']
            poi_url_list.append(full_url)
    next_page_exist = soup.find('a',{'class':"pagination-button next js-next"})
    if next_page_exist is None:
        break

for url in poi_url_list[0:5]:
    print(url)
print(len(poi_url_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Get Restaurant Info from restaurant page
# MAGIC 
# MAGIC ### Add check status like (裝修中) or (已搬遷) entries, if so, extract the status as well
# MAGIC 
# MAGIC ### Also need to filter out sponsored entries
# MAGIC 
# MAGIC After extraction, save them in a dataframe for later processing into CSV

# COMMAND ----------

# Access all URLs found from last step and extract required data from the page

emoji_names = ['Good', 'OK', 'Bad']
row_list = []

for url in poi_url_list:
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
    # Chinese3 name
    zh_name_container = soup.find('div', {"class": "poi-name"})
    zh_name = zh_name_container.find('span', {'class': "name"})
    # English name
    eng_name = soup.find('div', {"class": "smaller-font-name"})
    # Star rating
    star_rating = soup.find('div', {'class': 'header-score'})
    if star_rating is None:
        star_rating = ""
    else:
        star_rating = star_rating.text
    # Number of bookmark
    bookmark_count = soup.find('div', {'class': 'header-bookmark-count js-header-bookmark-count'})
    if bookmark_count is None:
        bookmark_count_text = ""
    else: 
        bookmark_count_text = bookmark_count.text
    # District
    district = soup.find('div', {'class': 'header-poi-district dot-separator'})
    # Price range
    price_range = soup.find('div', {'class': 'header-poi-price dot-separator'})
    # Cuisine type
    cuisine_type = soup.find('div', {'class': 'header-poi-categories dot-separator'})
    cuisine_type_list = cuisine_type.text.split('\n')
    cuisine_type_list = [x for x in cuisine_type_list if x]
    cuisine_type_list_string = '/'.join(cuisine_type_list)
    # Address (Chinese & English)
    address_div = soup.find('section', {'class': 'address-section'})
    addresses = address_div.find_all('div', {'class': 'content'})
    zh_address = addresses[0].text.strip()
    if len(addresses) > 1:
        eng_address = addresses[1].text.strip()
    else:
        eng_address = ""
    # Phone Number
    phone_number_div = soup.find('section', {'class': 'telephone-section'})
    if phone_number_div is not None:
        phone_number = phone_number_div.find('div', {'class': 'content'}).text
    else: 
        phone_number = ""
    # Smiley, OK, Cry Count
    emoji_count = soup.find_all('div', {"class": "score-div"})
#     print("Chinese name: " + zh_name.text)
#     print("English name: " + eng_name.text)
#     print("Star Rating: " + star_rating)
#     for i in range(3):
#         print(emoji_names[i] + ": " + emoji_count[i].text)
    # Special Status (closed down, renovating, etc...)
    special_stat = soup.find('span', {'class': 'poi-with-other-status'})
    if special_stat is None:
        special_stat = "OPERATING"
    else:
        special_stat = special_stat.text
    # Lat/Long?
    lat_long_div = soup.find('div', {'class': 'mapview-container'})
    if lat_long_div is not None:
        lat = lat_long_div.get("data-latitude")
        long = lat_long_div.get("data-longitude")
    else:
        lat = ""
        long = ""
    # Print the info out
#     print("Bookmark Count: " + bookmark_count.text)
#     print("District: " + district.text.strip())
#     print("Price Range: " + price_range.text.strip())
#     print("Cuisine: " + cuisine_type_list_string)
#     print("Chinese Address: " + zh_address)
#     print("English Address: " + eng_address)
#     print("Phone number: " + phone_number)
#     print("Special Status: " + special_stat)
#     print("----------------------------------")
    row_dict = {'name_1': zh_name.text, 'name_2': eng_name.text, 'star_rating': star_rating, 'bookmark_count': bookmark_count_text, 'district': district.text.strip(), 'cuisine_type': cuisine_type_list_string, 'address_1': zh_address, 'address_2': eng_address, 'phone_no': phone_number, 'good_count': emoji_count[0].text, 'ok_count': emoji_count[1].text, 'bad_count': emoji_count[2].text, 'status': special_stat, 'lat': lat, 'long': long, 'url': url}
    row_list.append(row_dict)
#     break
print(len(row_list))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Export the info into CSV
# MAGIC 
# MAGIC Make the file name contain the filter used (e.g. SSP_Western.csv)

# COMMAND ----------

# Write into DataFrame
poi_info_df = pd.DataFrame(row_list)

# COMMAND ----------

# Check head of dataframe
poi_info_df.head()

# COMMAND ----------

poi_info_df.shape

# COMMAND ----------

# Write dataframe to CSV
if len(row_list) > 0:
    cuisine_name = cuisine_type_df.filter(cuisine_type_df.cuisine_id == cuisine_id).collect()[0][-1]
    cuisine_name = cuisine_name.replace(" ", "")
    cuisine_name = cuisine_name.lower()
    district_name = dbutils.widgets.get("District Name")
    file_name = district_name + "_" + cuisine_name + ".csv"
    poi_info_df.to_csv("/dbfs/mnt/rex/" + file_name, index=False)
    print("Saved as " + file_name)
else:
    print("No matches")

# COMMAND ----------

# dbutils.fs.mount(
# source = "wasbs://04-publish@gpdl01pdseadfdl02.blob.core.windows.net/rex",
# mount_point = "/mnt/rex",
# extra_configs = {"fs.azure.account.key.gpdl01pdseadfdl02.blob.core.windows.net":dbutils.secrets.get(scope = "DATALAKE-PD", key = "azblob-pd02-key")}
# )

# COMMAND ----------

# to unmount any point
# dbutils.fs.unmount("/mnt/rex")

# COMMAND ----------

cuisine_id_spark_list = cuisine_type_df.select('cuisine_id').collect()
cuisine_id_list = [x[0] for x in cuisine_id_spark_list if x[0] < 10000]
# cuisine_id_list
cuisine_name_list = [cuisine_type_df.filter(cuisine_type_df.cuisine_id == x).collect()[0][-1] for x in cuisine_id_list]
# cuisine_name_list

# COMMAND ----------

# Search by district filter and cuisine filter to obtain URL to restaurant pages
district_id = 2008
district_name = location_df.filter(location_df.location_id == district_id).collect()[0][-1]
district_name = district_name.replace(" ", "_").lower()
for cuisine_id, cuisine_name in zip(cuisine_id_list, cuisine_name_list):
    print("Accessing info of {} restaurants in {} district".format(cuisine_name, district_name))
    poi_url_list = []
#     cuisine_id = cui
    for i in range(17):
        print("Scraping URLs from page " + str(i+1))
        request = urllib.request.Request("https://www.openrice.com/en/hongkong/restaurants?cuisineId=" + str(cuisine_id) + "&districtId=" + str(district_id) + "&page=" + str(i+1))
        request.add_header('User-Agent', 'Mozilla/5.0')
        response = urllib.request.urlopen(request)
        try:
            html = response.read()
        except:
            print("error happened when reading page " + str(i+1))
            continue
        soup = BeautifulSoup(html, 'html.parser')
        target = soup.findAll('div',{'class':"content-cell-wrapper"})
        for tar in target:
            if tar.find("span", {"class": "pois-restaurant-list-cell-sponsored"}) is None:
                res_url = tar.find("a",href=True)
                full_url = "https://www.openrice.com" + res_url['href']
                poi_url_list.append(full_url)
        next_page_exist = soup.find('a',{'class':"pagination-button next js-next"})
        if next_page_exist is None:
            break
#     for url in poi_url_list[0:5]:
#         print(url)
    print("{} {} restaurants found in {}".format(len(poi_url_list), cuisine_name, district_name))
    emoji_names = ['Good', 'OK', 'Bad']
    row_list = []
    for url in poi_url_list:
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
        # Chinese3 name
        zh_name_container = soup.find('div', {"class": "poi-name"})
        zh_name = zh_name_container.find('span', {'class': "name"})
        # English name
        eng_name = soup.find('div', {"class": "smaller-font-name"})
        # Star rating
        star_rating = soup.find('div', {'class': 'header-score'})
        if star_rating is None:
            star_rating = ""
        else:
            star_rating = star_rating.text
        # Number of bookmark
        bookmark_count = soup.find('div', {'class': 'header-bookmark-count js-header-bookmark-count'})
        if bookmark_count is None:
            bookmark_count_text = ""
        else: 
            bookmark_count_text = bookmark_count.text
        # District
        district = soup.find('div', {'class': 'header-poi-district dot-separator'})
        # Price range
        price_range = soup.find('div', {'class': 'header-poi-price dot-separator'})
        # Cuisine type
        cuisine_type = soup.find('div', {'class': 'header-poi-categories dot-separator'})
        cuisine_type_list = cuisine_type.text.split('\n')
        cuisine_type_list = [x for x in cuisine_type_list if x]
        cuisine_type_list_string = '/'.join(cuisine_type_list)
        # Address (Chinese & English)
        address_div = soup.find('section', {'class': 'address-section'})
        addresses = address_div.find_all('div', {'class': 'content'})
        zh_address = addresses[0].text.strip()
        if len(addresses) > 1:
            eng_address = addresses[1].text.strip()
        else:
            eng_address = ""
        # Phone Number
        phone_number_div = soup.find('section', {'class': 'telephone-section'})
        if phone_number_div is not None:
            phone_number = phone_number_div.find('div', {'class': 'content'}).text
        else: 
            phone_number = ""
        # Smiley, OK, Cry Count
        emoji_count = soup.find_all('div', {"class": "score-div"})
    #     print("Chinese name: " + zh_name.text)
    #     print("English name: " + eng_name.text)
    #     print("Star Rating: " + star_rating)
    #     for i in range(3):
    #         print(emoji_names[i] + ": " + emoji_count[i].text)
        # Special Status (closed down, renovating, etc...)
        special_stat = soup.find('span', {'class': 'poi-with-other-status'})
        if special_stat is None:
            special_stat = "OPERATING"
        else:
            special_stat = special_stat.text
        # Lat/Long?
        lat_long_div = soup.find('div', {'class': 'mapview-container'})
        if lat_long_div is not None:
            lat = lat_long_div.get("data-latitude")
            long = lat_long_div.get("data-longitude")
        else:
            lat = ""
            long = ""
        # Print the info out
    #     print("Bookmark Count: " + bookmark_count.text)
    #     print("District: " + district.text.strip())
    #     print("Price Range: " + price_range.text.strip())
    #     print("Cuisine: " + cuisine_type_list_string)
    #     print("Chinese Address: " + zh_address)
    #     print("English Address: " + eng_address)
    #     print("Phone number: " + phone_number)
    #     print("Special Status: " + special_stat)
    #     print("----------------------------------")
        row_dict = {'name_1': zh_name.text, 'name_2': eng_name.text, 'star_rating': star_rating, 'bookmark_count': bookmark_count_text, 'district': district.text.strip(), 'cuisine_type': cuisine_type_list_string, 'address_1': zh_address, 'address_2': eng_address, 'phone_no': phone_number, 'good_count': emoji_count[0].text, 'ok_count': emoji_count[1].text, 'bad_count': emoji_count[2].text, 'status': special_stat, 'lat': lat, 'long': long, 'url': url}
        row_list.append(row_dict)
    #     break
    print(len(row_list))
    # Write into DataFrame
    poi_info_df = pd.DataFrame(row_list)
    # Write dataframe to CSV
    if len(row_list) > 0:
        cuisine_name = cuisine_type_df.filter(cuisine_type_df.cuisine_id == cuisine_id).collect()[0][-1]
        cuisine_name = cuisine_name.replace(" ", "")
        cuisine_name = cuisine_name.lower()
#         district_name = dbutils.widgets.get("District Name")
        file_name = district_name + "_" + cuisine_name + ".csv"
        poi_info_df.to_csv("/dbfs/mnt/rex/" + file_name, index=False)
        print("Saved as " + file_name)
    else:
        print("No results, continuing...")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Cleaning
# MAGIC Check duplicates and combine CSVs by districts

# COMMAND ----------

import os
os.listdir('/dbfs/mnt/rex/')

# COMMAND ----------

for item in os.listdir('/dbfs/mnt/rex/'):
    print("Processing CSVs from {} folder".format(item))
    tmp_df = pd.DataFrame()
    if "." not in item:
        csv_list = os.listdir('/dbfs/mnt/rex/' + item)
        for csv in csv_list:
            small_df = pd.read_csv('/dbfs/mnt/rex/' + item + "/" + csv)
            tmp_df = tmp_df.append(small_df, ignore_index=True)
        file_name = item + ".csv"
        print(tmp_df.shape)
        tmp_df = tmp_df.drop_duplicates(subset=['url'], ignore_index=True)
        print(tmp_df.shape)
        tmp_df.to_csv("/dbfs/mnt/rex/" + file_name)
        print("Saved combined CSV as {}".format(file_name))

# COMMAND ----------

import os
os.listdir('/dbfs/mnt/rex/combined_csv')

# COMMAND ----------

folder_path = '/dbfs/mnt/rex/combined_csv/'
tmp_df = pd.DataFrame()
for item in os.listdir(folder_path):
    print("Processing {}".format(item))
    small_df = pd.read_csv('/dbfs/mnt/rex/combined_csv/' + item)
    tmp_df = tmp_df.append(small_df, ignore_index=True)
    print(tmp_df.shape)

# COMMAND ----------

tmp_df.head(10)

# COMMAND ----------

tmp_df = tmp_df.drop(columns=['Unnamed: 0', 'Unnamed: 0.1'])
tmp_df.head(10)

# COMMAND ----------

tmp_df['phone_no'] = tmp_df['phone_no'].astype('str')
tmp_df['phone_no'] = tmp_df['phone_no'].apply(lambda x: x[:-2] if "." in x else x)
tmp_df['phone_no'] = tmp_df['phone_no'].apply(lambda x: ''.join(e for e in x if e.isnumeric()))
tmp_df.head(10)

# COMMAND ----------

file_name = "final_openrice_data_wo_polygon.csv"
print(tmp_df.shape)
tmp_df = tmp_df.drop_duplicates(subset=['url'], ignore_index=True)
print(tmp_df.shape)
tmp_df.to_csv("/dbfs/mnt/rex/" + file_name, index=False)
print("Saved combined CSV as {}".format(file_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add demographic data

# COMMAND ----------

import geojson

# JSON file
f = open ('/dbfs/mnt/rex/geo_json/districts.json', "r")
 
# Reading from file
data = geojson.loads(f.read())

print(geojson.dumps(data, indent=4))
 
# Closing file
f.close()

# COMMAND ----------

data['features'][0]['properties']

# COMMAND ----------

# Check if a point is in the polygon or not
from shapely.geometry import shape, Point
point = Point(114.15252, 22.282554)

for feature in data['features']:
    polygon = shape(feature['geometry'])
    if polygon.contains(point):
        print('Found containing polygon: ', feature['properties']['District_NAME'])
    else:
        print('Not in polygon: ', feature['properties']['District_NAME'])

# COMMAND ----------

for feature in data['features']:
    print(feature['properties']['LSB_CODE'])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Add District Data

# COMMAND ----------

import geojson

# JSON file
f = open ('/dbfs/mnt/rex/geo_json/districts.json', "r")
 
# Reading from file
data = geojson.loads(f.read())
 
# Closing file
f.close()

# COMMAND ----------

# Check if a point is in the polygon or not
from shapely.geometry import shape, Point

restaurant_df = pd.read_csv('/dbfs/mnt/rex/final_openrice_data_wo_polygon.csv')
# restaurant_df.head(10)
long_list = restaurant_df['long'].tolist()
lat_list = restaurant_df['lat'].tolist()
print(len(long_list))
print(len(lat_list))

# COMMAND ----------

district_result_list = []
for (long, lat) in zip(long_list, lat_list):
    point = Point(long, lat)
    for feature in data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            print('Found containing polygon: ', feature['properties']['District_NAME'])
            district_result_list.append(feature['properties']['District_NAME'])
            break
            
district_result_list[:5]

# COMMAND ----------


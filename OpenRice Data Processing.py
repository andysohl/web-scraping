# Databricks notebook source
# MAGIC %md
# MAGIC # OpenRice Data Processing
# MAGIC Adding demographic data to enrich OpenRice data

# COMMAND ----------

import pandas as pd
import json
import geojson
import csv

# COMMAND ----------

def progress(count, total, status=''):
  bar_len = 60
  filled_len = int(round(bar_len * count / float(total)))
  
  percents = round(100.0 * count / float(total), 1)
  bar = '=' * filled_len + '-' * (bar_len - filled_len)

#   clear_output(wait=True)
  print('[{}] {}{} ...{}\r'.format(bar, percents, '%', status))

# COMMAND ----------

# Check if a point is in the polygon or not
from shapely.geometry import shape, Point

restaurant_df = pd.read_csv('/dbfs/mnt/rex/openrice/final_openrice_data_wo_polygon.csv')
restaurant_df['openrice_district'] = restaurant_df['district']
long_list = restaurant_df['long'].tolist()
lat_list = restaurant_df['lat'].tolist()
print(len(long_list))
print(len(lat_list))

# COMMAND ----------

lat_long_df = pd.DataFrame()
lat_long_df['long'] = long_list
lat_long_df['lat'] = lat_list
lat_long_df

# COMMAND ----------

lat_long_df.to_csv('/dbfs/mnt/rex/openrice/lat_long_list.csv', index=False)

# COMMAND ----------

restaurant_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Districts Data (18 districts)

# COMMAND ----------

# JSON file
f = open ('/dbfs/mnt/rex/geo_json/districts.json', "r")
 
# Reading from file
district_data = geojson.loads(f.read())

print(geojson.dumps(district_data, indent=4))
 
# Closing file
f.close()

# COMMAND ----------

district_result_list = []
count = 0
total = len(long_list)
for (long, lat) in zip(long_list, lat_list):
    point = Point(long, lat)
    matched = False
    for feature in district_data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            print('Found containing polygon: ', feature['properties']['District_NAME'])
            district_result_list.append(feature['properties']['District_NAME'])
            matched = True
            break
    if not matched:
        print("No physical location")
        district_result_list.append("nan")
    count += 1
    progress(count, total, "of restaurants matched")
            
district_result_list[:5]

# COMMAND ----------

len(district_result_list)

# COMMAND ----------

restaurant_df['district'] = district_result_list
restaurant_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Add subdistrict data

# COMMAND ----------

# JSON file
f = open ('/dbfs/mnt/rex/geo_json/subdistricts.json', "r")
 
# Reading from file
subdistrict_data = geojson.loads(f.read())

# print(geojson.dumps(subdistrict_data, indent=4))
print(geojson.dumps(subdistrict_data['features'][0], indent=4))
 
# Closing file
f.close()

# COMMAND ----------

for features in subdistrict_data['features']:
  print(features['properties']['DC_NAME'])

# COMMAND ----------

subdistrict_result_list = []
count = 0
total = len(long_list)
for (long, lat) in zip(long_list, lat_list):
    point = Point(long, lat)
    matched = False
    for feature in subdistrict_data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            print('Found containing polygon: ', feature['properties']['DC_NAME'])
            subdistrict_result_list.append(feature['properties']['DC_NAME'])
            matched = True
            break
    if not matched:
        print("No physical location")
        subdistrict_result_list.append("nan")
    count += 1
    progress(count, total, "of restaurants matched")
            
subdistrict_result_list[:5]

# COMMAND ----------

len(subdistrict_result_list)

# COMMAND ----------

restaurant_df['subdistrict'] = subdistrict_result_list
restaurant_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Population Area

# COMMAND ----------

# JSON file
f = open ('/dbfs/mnt/rex/geo_json/population_areas.json', "r")
 
# Reading from file
pop_area_data = geojson.loads(f.read())

# print(geojson.dumps(subdistrict_data, indent=4))
print(geojson.dumps(pop_area_data['features'][0], indent=4))
 
# Closing file
f.close()

# COMMAND ----------

for features in pop_area_data['features']:
    print(features['properties']['Area_Name'])

# COMMAND ----------

pop_area_result_list = []
count = 0
total = len(long_list)
for (long, lat) in zip(long_list, lat_list):
    point = Point(long, lat)
    matched = False
    for feature in pop_area_data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            print('Found containing polygon: ', feature['properties']['Area_Name'])
            pop_area_result_list.append(feature['properties']['Area_Name'])
            matched = True
            break
    if not matched:
        print("No physical location")
        pop_area_result_list.append("nan")
    count += 1
    progress(count, total, "of restaurants matched")
            
pop_area_result_list[:5]

# COMMAND ----------

len(pop_area_result_list)

# COMMAND ----------

len(pop_area_data['features'])

# COMMAND ----------

restaurant_df['population_area'] = pop_area_result_list
restaurant_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Major Housing Area

# COMMAND ----------

# JSON file
f = open ('/dbfs/mnt/rex/geo_json/major_housing_areas.json', "r")
 
# Reading from file
major_housing_area_data = geojson.loads(f.read())

# print(geojson.dumps(subdistrict_data, indent=4))
print(geojson.dumps(major_housing_area_data['features'][0], indent=4))
 
# Closing file
f.close()

# COMMAND ----------

for features in major_housing_area_data['features']:
    print(features['properties']['Area_Name'])

# COMMAND ----------

major_housing_area_result_list = []
count = 0
total = len(long_list)
for (long, lat) in zip(long_list, lat_list):
    point = Point(long, lat)
    matched = False
    for feature in major_housing_area_data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            print('Found containing polygon: ', feature['properties']['Area_Name'])
            major_housing_area_result_list.append(feature['properties']['Area_Name'])
            matched = True
            break
    if not matched:
        print("No physical location")
        major_housing_area_result_list.append("nan")
    count += 1
    progress(count, total, "of restaurants matched")
            
major_housing_area_result_list[:5]

# COMMAND ----------

len(major_housing_area_result_list)

# COMMAND ----------

restaurant_df['major_housing_area'] = major_housing_area_result_list
restaurant_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Catchment Area

# COMMAND ----------

# JSON file
f = open ('/dbfs/mnt/rex/geo_json/catchment_areas_2.json', "r")
 
# Reading from file
catchment_areas_data = geojson.loads(f.read())

# print(geojson.dumps(subdistrict_data, indent=4))
print(geojson.dumps(catchment_areas_data['features'][0], indent=4))
 
# Closing file
f.close()

# COMMAND ----------

for features in catchment_areas_data['features']:
    print(features['properties']['Area_Name'])

# COMMAND ----------

len(catchment_areas_data['features'])

# COMMAND ----------

catchment_areas_result_list = []
count = 0
total = len(long_list)
for (long, lat) in zip(long_list, lat_list):
    point = Point(long, lat)
    matched = False
    for feature in catchment_areas_data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            print('Found containing polygon: ', feature['properties']['Area_Name'])
            catchment_areas_result_list.append(feature['properties']['Area_Name'])
            matched = True
            break
    if not matched:
        print("No physical location")
        catchment_areas_result_list.append("nan")
    count += 1
    progress(count, total, "of restaurants matched")
            
catchment_areas_result_list[:5]

# COMMAND ----------

len(catchment_areas_result_list)

# COMMAND ----------

restaurant_df['catchment_area'] = catchment_areas_result_list
restaurant_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Street Blocks

# COMMAND ----------

# JSON file
f = open ('/dbfs/mnt/rex/geo_json/street_blocks.json', "r")
 
# Reading from file
street_blocks_data = geojson.loads(f.read())

# print(geojson.dumps(subdistrict_data, indent=4))
print(geojson.dumps(street_blocks_data['features'][0], indent=4))
 
# Closing file
f.close()

# COMMAND ----------

for features in street_blocks_data['features']:
    print(features['properties']['SB_LINK'])

# COMMAND ----------

len(street_blocks_data['features'])

# COMMAND ----------

street_blocks_result_list = []
count = 0
total = len(long_list)
for (long, lat) in zip(long_list, lat_list):
    point = Point(long, lat)
    matched = False
    for feature in street_blocks_data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            print('Found containing polygon: ', feature['properties']['SB_LINK'])
            street_blocks_result_list.append(feature['properties']['SB_LINK'])
            matched = True
            break
    if not matched:
        print("No physical location")
        street_blocks_result_list.append("nan")
    count += 1
    progress(count, total, "of restaurants matched")
            
street_blocks_result_list[:5]

# COMMAND ----------

len(street_blocks_result_list)

# COMMAND ----------

restaurant_df['street_blocks'] = street_blocks_result_list
restaurant_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Large Street Blocks (2019 ver.?)

# COMMAND ----------

# JSON file
f = open ('/dbfs/mnt/rex/geo_json/large_street_blocks_2019.json', "r")
 
# Reading from file
large_street_blocks_data = geojson.loads(f.read())

# print(geojson.dumps(subdistrict_data, indent=4))
print(geojson.dumps(large_street_blocks_data['features'][0], indent=4))
 
# Closing file
f.close()

# COMMAND ----------

for features in large_street_blocks_data['features']:
    print(features['properties']['LSB_CODE'])

# COMMAND ----------

len(large_street_blocks_data['features'])

# COMMAND ----------

large_street_blocks_result_list = []
count = 0
total = len(long_list)
for (long, lat) in zip(long_list, lat_list):
    point = Point(long, lat)
    matched = False
    for feature in large_street_blocks_data['features']:
        polygon = shape(feature['geometry'])
        if polygon.contains(point):
            print('Found containing polygon: ', feature['properties']['LSB_CODE'])
            large_street_blocks_result_list.append(feature['properties']['LSB_CODE'])
            matched = True
            break
    if not matched:
        print("No physical location")
        large_street_blocks_result_list.append("nan")
    count += 1
    progress(count, total, "of restaurants matched")
            
large_street_blocks_result_list[:5]

# COMMAND ----------

len(large_street_blocks_result_list)

# COMMAND ----------

restaurant_df['large_street_blocks'] = large_street_blocks_result_list
restaurant_df.head()

# COMMAND ----------

restaurant_df.to_csv('/dbfs/mnt/rex/final_openrice_data_20220104.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Cleansing for uploading to data lake

# COMMAND ----------

restaurant_df = pd.read_csv('/dbfs/mnt/rex/final_openrice_data_20220104.csv', index_col=0)
restaurant_df.head()

# COMMAND ----------

restaurant_df.isna().sum()

# COMMAND ----------

cuisine_list = []
for cuisine in restaurant_df['cuisine_type'].unique():
    cuisine_small_list = cuisine.split('/')
    for item in cuisine_small_list:
        cuisine_list.append(item)
cuisine_list = list(dict.fromkeys(cuisine_list))
print(len(cuisine_list))
for item in cuisine_list:
    print(item)

# COMMAND ----------

cuisine_df.to_csv('/dbfs/mnt/rex/cuisine_type.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Matching restaurants with cuisine type (by their unique key)

# COMMAND ----------

poi_cuisine_df = restaurant_df[['name_1', 'cuisine_type']]
poi_cuisine_df.head()

# COMMAND ----------

poi_cuisine_list = []
for i in range(len(poi_cuisine_df['name_1'])):
    cuisine_cell = poi_cuisine_df.iloc[i, 1]
    cuisine_split_list = cuisine_cell.split("/")
    for cuisine in cuisine_split_list:
        print(cuisine)
        cuisine_index = cuisine_list.index(cuisine)
        matching_tuple = (i+1, cuisine_index)
        poi_cuisine_list.append(matching_tuple)
        print(matching_tuple)

# COMMAND ----------

matching_df = pd.DataFrame(poi_cuisine_list)
matching_df

# COMMAND ----------

matching_df.to_csv('/dbfs/mnt/rex/poi_cuisine_matching.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Save GEOJSON as CSV

# COMMAND ----------

# JSON file
f = open ('/dbfs/mnt/rex/geo_json/districts.json', "r")
 
# Reading from file
data = geojson.loads(f.read())

print(geojson.dumps(data['features'][0], indent=4))
 
# Closing file
f.close()

# COMMAND ----------

for item in data['features'][0]['properties']:
    print(item + ": " + data['features'][0]['properties'][item])

# COMMAND ----------

dict_list = []
for item in data['features']:
    dict_list.append(item['properties'])
len(dict_list)

# COMMAND ----------

df = pd.DataFrame(dict_list)
df.head()

# COMMAND ----------

for i in range(df.shape[0]):
    print(len(df.iloc[i, -1]))

# COMMAND ----------

df.to_csv('/dbfs/mnt/rex/geo_csv/100m_hexagon.csv', index=False)

# COMMAND ----------


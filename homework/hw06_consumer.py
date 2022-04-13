#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
spark= SparkSession.builder.getOrCreate()

stream_df = (spark.readStream.format('socket')
                             .option('host', 'localhost')
                             .option('port', 22223)
                             .load())

json_df = stream_df.selectExpr("CAST(value AS STRING) AS payload")

writer = (
    json_df.writeStream
           .queryName('ISSLocation')
           .format('memory')
           .outputMode('append')
)

streamer = writer.start()

import time

for _ in range(5):
    df = spark.sql("""
    SELECT get_json_object(payload, '$.iss_position') AS iss_position,
           CAST(get_json_object(payload, '$.timestamp') AS INTEGER) AS timestamp
    FROM ISSLocation
    """)
    
    #df.show(10)
    
    #print(df)
    time.sleep(5)
    
streamer.awaitTermination(timeout=3600)
print('streaming done!')



pdDF = df.toPandas()
print('Shape of dataframe :',pdDF.shape)


lat_lon_df=pdDF['iss_position'].str.split(',', expand=True)
lat_lon_df.columns = ['lon', 'lat']
lat_lon_df['Time']= pdDF['timestamp']

# converting the timestamp format
import time
time_list=[]
for (idx, row) in lat_lon_df.iterrows():
    
    s=time.gmtime(row.loc['Time'])
    time_list.append(time.strftime("%Y-%m-%d %H:%M:%S", s))
    
lat_lon_df['Conveted_Time']=time_list

# stripping unwanted data
lat_lon_df['lat'] = lat_lon_df['lat'].str.replace(r'[a-z]', '')
lat_lon_df['lat'] = lat_lon_df['lat'].map(lambda x: x.lstrip(r'":').rstrip('"}'))
lat_lon_df['lon'] = lat_lon_df['lon'].str.replace(r'[a-z]', '')
lat_lon_df['lon'] = lat_lon_df['lon'].map(lambda x: x.lstrip(r'"{:').rstrip('"'))


# converting values to numeric
lat_lon_df["lat"] = pd.to_numeric(lat_lon_df["lat"])
lat_lon_df["lon"] = pd.to_numeric(lat_lon_df["lon"])

#lat_lon_df.to_csv('ISS-Location_data.csv',index=False)


#plotting the graph
import pandas as pd
import geopandas
import matplotlib.pyplot as plt
gdf = geopandas.GeoDataFrame(
    lat_lon_df, geometry=geopandas.points_from_xy(lat_lon_df['lon'], lat_lon_df['lat']))

world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
plt.rcParams["figure.figsize"] = (20,19)
base = world.plot(color='white', edgecolor='black',legend=True)

gdf.plot(ax=base, marker='o', color='red', markersize=20);
plt.title('LOCATION of ISS (2022-04-12   21:15:21 to 22:15:20 UTC)', fontsize= 25)
plt.xlabel('Longitude', fontsize = 20)
plt.ylabel('Latitude', fontsize = 20)
plt.savefig('Iss_Loc_world.jpg')
plt.show()






















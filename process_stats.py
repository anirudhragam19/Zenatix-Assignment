#imports
import psutil
import elasticsearch
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json



# Creating a list to store all the processes
process_list = []

# Iterating through all the processes in the system
for process in psutil.process_iter():
    
    process_info = dict()
    
    
    # Collecting information about each process and storing it in thh form of a dictionary
    process_info = process.as_dict(attrs=['pid','name','num_threads','memory_percent','cpu_percent'])
    process_list.append(process_info)
    

# Storing the collected information in a tabular manner using pandas
df = pd.DataFrame(process_list)


# Removing rows with NaN values
df.dropna(subset = ['cpu_percent','memory_percent'], inplace=True)

# Resetting index of the dataframe after dropping rows with NaN
df.reset_index(drop=True,inplace=True)

#Adding an index column
df['Index'] = df.index



# Uploading the dataframe to Elastic Search

# Connecting to the Elastic search deployment via the cloud id
es = Elasticsearch(hosts = 'http://localhost:9200/')


# Defining the mappings for the index
body = {
        "mappings":{
            
            "properties": {
                
                "pid":{"type" : "integer"},
                "name":{"type" : "text"},
                "num_threads":{"type" : "float"},
                "memory_percent":{"type" : "float"},
                "cpu_percent":{"type" : "float"},
                
                }
            }
        }


# Creating an index with the above mappings
es.indices.create(index = 'process_stats_5.0',body=body)

# Converting the data into a form that is acceptable by ElasticSearch

data = []

for index,row in df.iterrows():

    proc = {}
    proc['Index'] = row['Index']
    proc['name'] = row['name']
    proc['pid'] = row['pid']
    proc['num_threads'] = row['num_threads']
    proc['cpu_percent'] = row['cpu_percent']
    proc['memory_percent'] = row['memory_percent']
    
    data.append(proc)
    
# Sending each document/row to ElasticSearch
for doc_i in data:
    es.index(index='process_stats_5.0',doc_type='_doc',id = doc_i['Index'],body=doc_i)

# Sanity check for proper storage of data on ElasticSearch
# Fetching the data item with ID 1
res = es.get(index="process_stats_5.0", doc_type="_doc", id=1)
print(res)

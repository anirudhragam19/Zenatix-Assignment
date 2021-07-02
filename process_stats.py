# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
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








# Converting df back to dictionary














# Uploading the dataframe to Elastic Search

# Connecting to the Elastic search deployment via the cloud id
es = Elasticsearch(hosts = 'http://localhost:9200/')





keys = ['pid','name','num_threads','memory_percent','cpu_percent']




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


#es.indices.create(index = 'process_stats_5.0',body=body)



'''
keys = ['pid','memory_percent','cpu_percent','num_threads']


def filterKeys(document):
    return {key: document[key] for key in keys }

def generator(df):
    
    df_iter = df.iterrows()
    
    
    for index, document in df_iter:
        
        yield {
            '_index': 'process_stats_5.0',
            '_id' : f"{document['Index']}",
            '_type':'_doc',
            '_source': filterKeys(document)
                
                
                
            }
        raise StopIteration
        
  '''
l = []
'''
for index,row in df.iterrows():

    d = {}
    d['Index'] = row['Index']
    d['pid'] = row['pid']
    d['num_threads'] = row['num_threads']
    d['cpu_percent'] = row['cpu_percent']
    d['memory_percent'] = row['memory_percent']
    
    l.append(d)
    

for doc_i in l:
    
   
    es.index(index='process_stats_5.0',doc_type='_doc',id = doc_i['Index'],body=doc_i)
'''
res = es.get(index="process_stats_5.0", doc_type="_doc", id=1)
print(res)

    
 





















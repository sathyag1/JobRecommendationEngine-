from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl import connections
from elasticsearch import helpers
import elasticsearch.helpers
import pandas as pd
import os
import json
from pandas import json_normalize
from dotenv import load_dotenv

load_dotenv()

import CommonLayer as CL
import BusinessLayer as BL

Elk_Endpoint = os.getenv('ELK_ENDPOINT')
ELK_USERNAME = os.getenv('ELK_USERNAME')
ELK_PASSWORD = os.getenv('ELK_PASSWORD')
ELK_INDEX = os.getenv('ELK_INDEX')

es=connections.create_connection(hosts=[Elk_Endpoint],timeout=1200,http_auth=(ELK_USERNAME,ELK_PASSWORD))

es.ping()

def getelkdata(must_query,should_query):
    query_body = {
      "query": {
                        "bool": {
                            "filter": [
                                            {
                                                "exists": {
                                                    "field": "SKILLS_VEC"
                                                }
                                            }
                                    ],
                            "must" : 
                               must_query
                                ,
#                             Loose Matches
                             "should": 
                                 should_query
                        }
                    }
                }
    dfresult = getdataset(query_body)
    print(query_body)
    return dfresult

def getfilterelkdata(gender,skillset,matchquery,matchqueryforshould):
        query_body = {
                "query": {
                                    "bool": {
                                        "filter": [
                                                            {
                                                                "exists": {
                                                                    "field": "SKILLS_VEC"
                                                                }
                                                            }
                                                    ],
                                        "must" : 
                                        matchquery
                                        ,
                                        "should":
                                        matchqueryforshould

                                    }
                            }
                    }
                   
        dfresult = getdataset(query_body)
        print(query_body)
        return dfresult

def getsavedjoblistfrom_elk(jobno_list):
    query_body = {
                "query": {
                                    "bool": {
                                        "filter": [
                                                            {
                                                                "exists": {
                                                                    "field": "JOB_NUMBER"
                                                                }
                                                            }
                                                    ],
                                        "must" : 
                                        [   {"term": { "ACTIVE_FLAG": "true"}},
                                            {"terms": { "JOB_NUMBER": jobno_list}}]
                                    }
                            }
                    }
                   
    dfresult = getdataset(query_body)
    print(query_body)
    return dfresult
    

def getdataset(query_body):
    response = es.search(index=ELK_INDEX,body=query_body,size=400)
    print ("documents returned:", len(response["hits"]["hits"]))
    data=""
    data = [x['_source'] for x in response["hits"]["hits"]]
    json_string=""
    json_string = json.dumps(data)
    dfresult = pd.read_json(json_string, orient ='records')
    print("gotdata")
    return dfresult

#freetext functions
def getfreetextdata(matchquery,must_matchquery,matchqueryfilter):
    print("data layer query formation")
        
    query_body = {
      "query": { 
          "bool": {
            "should":matchquery,
            "must":must_matchquery,
            "filter":matchqueryfilter
          }     
      },
      "sort": [
        {
          "_score": {
            "order": "desc"
          }
        }]
      }
    dfresult = getdataset(query_body)
    print(query_body)
    print(dfresult.shape)
    return dfresult


    
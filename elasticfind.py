import pandas as pd
import elasticsearch
from elasticsearch import Elasticsearch
class Elasticfind:
    def __init__(self, index):
        self.index = index
        self.initialize()
        self.top_results = []
        self.df = self.read_and_preprocess()
    
    def initialize(self):
        '''initializes elasticsearch interface with python & creates index of passed name
           returns True if online
        '''
        self.es = Elasticsearch(hosts='http://localhost:9200')
        self.es.indices.create(self.index)
        return self.es.ping()
        
    def read_and_preprocess(self, path_to_df='netflix_titles.csv'):
        '''reads & drops na columns from dataset, converts into records
           returns df
        '''
        df = pd.read_csv(path_to_df)
        df = df.dropna()
        df = df.to_dict('records')
        return df
    
    def generator(self):
        '''generator function for producing documents
           yields documents one by one
        '''
        for idx, src in enumerate(self.df):
            yield {
                "_index":"netflix",
                "_id":idx,
                "_source":{
                    "title":src["title"],
                    "director":src["director"],
                    "rating":src["rating"],
                    "description":src["description"],
                    "duration":src["duration"],
                    "cast":src["cast"]
                }
            }
            
    def ingest_dataset(self):
        '''Uses bulk API for ingesting documents in elasticsearch
           consumed by start_indexing
           returns result of the operation
        '''
        from elasticsearch.helpers import bulk
        res = bulk(self.es, self.generator())
        return res
    
    def start_indexing(self):
        '''start indexing documents into elasticsearch
           returns "success" otherwise error
        '''
        try:
            self.ingest_dataset()
            return "success"
        except Exception as e:
            return str(e)
        
    def get_fields(self):
        '''returns list of fields(columns) in dataset
           
        '''
        return list(self.es.indices.get_mapping(index=self.index)[self.index]['mappings']['properties'].keys())

    def is_ready(self):
        '''returns True if index is present else False
        '''
        return self.es.indices.exists(index=self.index)
        
    def body_generator(self, field:str, value:str, matchtype=None, size=None, src=None):
        '''generates body for single pair - field:text
           consumed by search engine
        '''
        body = dict()
        if src:
            body['_source'] = src
        if size:
            body['size'] = size
        if matchtype:
            body['query'] = dict()
            body['query'][matchtype] = {field:value}
        if not body:
            return None
        return body
    
    def find(self, keywords:str):
        '''returns list of hits for each field
           consumed by process_result
        '''
        result = []
        for field in self.get_fields():
            b = self.body_generator(field=field,value=keywords, matchtype='match',size=self.es.count(index=self.index)['count'])
            result.append(self.es.search(index=self.index,body=b))
        return result
    
    def process_result(self, res:list):
        '''takes in result from find and processes
           returns top results from found hits
        '''
        documents = dict()
        for idx, field in enumerate(range(len(self.get_fields()))):
            for doc in range(res[field]['hits']['total']['value']):
                documents[res[field]['hits']['hits'][doc]['_id']] = res[field]['hits']['hits'][doc]['_score']
        sorted_responses = sorted(documents.items(),key=lambda x:x[1], reverse=True)
        for idx, score in sorted_responses:
            self.top_results.append(self.es.get(index=self.index,id=idx)['_source'])
        return self.top_results
    
    def count_documents(self):
        return len(self.top_results)
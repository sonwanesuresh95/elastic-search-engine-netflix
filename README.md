# elastic-search-engine-netflix
elasticsearch engine built on the top of netflix dataset to retrieve records from dataset relevant to user queries based on 'cast', 'description', 'director', 'duration', 'rating', 'title' etc

## Info
This project can <br>
1. Index records from netflix dataset to elasticsearch locally<br>
2. Search and retrieve relevant records with respect to queries
3. Generate body of query DSL
4. Show count of matched records

## Usage
Open Terminal and run:
```
$ <path_to>/elasticsearch-7.9.3/bin/elasticsearch
```
Make sure its up and running<br>

Use Python terminal / Jupyter Notebook<br>
```
# create instance of Elasticfind class
ef = Elasticfind(index='netflix')
```
Create index and ingest the data into it
```
# index netflix dataset records into elasticsearch
ef.start_indexing()
```
Retrieve records from index by doing:
```
response = ef.find('<some_query>')
top_results = ef.process_result(response)
print(top_results)
```

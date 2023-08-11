## elasticsearch-test
Setting up a local elasticsearch server with docker

## How to use
Initialize the elastic client with the following method, this will create the elasticsearch server of the version 8.4.3 
with docker
```
val esClient = ElasticsearchTestServer.global.client
```
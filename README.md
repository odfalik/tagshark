# Tagshark


### Getting Started

```shell
pip install -r requirements.txt
sudo systemctl restart elasticsearch
sudo systemctl restart kibana
```

To import Kibana dashboard/maps:
1. Go to Kibana
2. Click on Management
3. Click on Saved Objects
4. Click on the Import button
5. Load ```export.ndjson``` (found in the root of this repo)

### Useful ports
- ```4040``` Spark
- ```9200``` ES
- ```5601``` Kibana
    

### Useful ES queries
```json
// Delete index
DELETE tagshark

// Create tagshark index
PUT tagshark

// Set geo_point mapping for location field
PUT tagshark/_mappings
{
  "properties": {
    "location": {
      "type": "geo_point"
    }
  }
}

// Get number of indexed documents
GET tagshark/_count

// Get a random document from tagshark index
GET tagshark/_search
{
  "size": 1,
  "query": {
      "function_score": {
        "query" : { "match_all": {} },
        "random_score": {}
      }
  }
}
```

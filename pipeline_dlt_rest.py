import dlt 
from dlt.sources.rest_api import rest_api_source 
client={"base_url": "https://api.fda.gov/"}
config= {
    "client": client,
    "resources":[
    {
           "name": "reports",
"endpoint":{
    "path": "drug/event.json",
    "params": {
        "limit": 5
    }, 
    "data_selector": "results",
    "paginator":{
        "type": "single_page"}
} 

    }          ]
}
source = rest_api_source(config)
pipeline=dlt.pipeline(
pipeline_name="openfda_dlt_rest",
destination="duckdb",
dataset_name="openfda_rest"
)
load_info=pipeline.run(source, write_disposition="replace")
print(load_info)
from __future__ import print_function
import json
import urllib
import base64
import os
import requests
from urllib.parse import urlencode
from google.cloud import bigquery

dataset_name = 'invoice_parser_results'
table_name = 'knowledge_graph_details'
my_schema = [
{
   "name": "input_file_name",
   "type": "STRING"
 },{
   "name": "entity_type",
   "type": "STRING"
 },{
   "name": "entity_text",
   "type": "STRING"
 },{
   "name": "name",
   "type": "STRING"
 },{
   "name": "url",
   "type": "STRING"
 },{
   "name": "description",
   "type": "STRING"
 },{
   "name": "result_score",
   "type": "STRING"
 }
]

bq_client = bigquery.Client()
 
def write_to_bq(dataset_name, table_name, kg_response_dict, my_schema):
  dataset_ref = bq_client.dataset(dataset_name)
  table_ref = dataset_ref.table(table_name)
  row_to_insert =[]
  row_to_insert.append(kg_response_dict)

  json_data = json.dumps(row_to_insert, sort_keys=False)
  #Convert to a JSON Object
  json_object = json.loads(json_data)
  print(json_object)
  job_config = bigquery.LoadJobConfig(schema = my_schema)
  job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON

  job = bq_client.load_table_from_json(json_object, table_ref, job_config=job_config)
  error = job.result()  # Waits for table load to complete.
  print(error)
  
 
def get_kg_data(event, context):
  """Triggered from a message on a Cloud Pub/Sub topic.
  Args:
  event (dict): Event payload.
  context (google.cloud.functions.Context): Metadata for the event.
  """
  pubsub_message = base64.b64decode(event['data']).decode('utf-8')
  #print(type(pubsub_message))
  #print(pubsub_message)
  message_dict = json.loads(pubsub_message)
  query = message_dict.get('entity_text')
  kg_dict = {} 
  kg_dict["input_file_name"] = message_dict.get('input_file_name')
  kg_dict["entity_type"] = message_dict.get('entity_type')
  kg_dict["entity_text"] = query
  kg_response_dict = extract_kg_info(query)
  kg_dict.update(kg_response_dict)
  print(kg_dict)

  write_to_bq(dataset_name, table_name, kg_dict, my_schema)
  #print(geocode_response_dict)

# Using Geocoding API 
def extract_kg_info(query,data_type='json'):
  kg_response_dict = {} 
  service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
  API_key = os.environ.get('API_key')
  params = {
  'query': query,
  'limit': 1,
  'types': ['Organization', 'GovernmentOrganization'],
  'indent': True,
  'key': API_key,
  }
  print(query)
  
  url = service_url + '?' + urlencode(params)
  r = requests.get(url)
  print(url)

  if (len(r.json()['itemListElement']) > 0):
    response = r.json()['itemListElement'][0]   

  else:
    return {}
  
  #r = requests.get(url)
  print(r.status_code)
  if r.status_code not in range(200,299):
    print('status code not in range')
    return {}
  try:
    kg_response_dict["name"] = response['result']['name']
    kg_response_dict["url"] = response['result']['url']
    kg_response_dict["description"] = response['result']['description']
    kg_response_dict["result_score"] = str(response.get("resultScore"))
    print(kg_response_dict)

  except:
      pass
  return kg_response_dict

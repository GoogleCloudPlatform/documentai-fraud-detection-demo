import base64
import re
import os
import json
from datetime import datetime
from google.cloud import bigquery
from google.cloud import documentai_v1beta3 as documentai
from google.cloud import storage
from google.cloud import pubsub_v1
 
#Reading environment variables
gcs_output_uri_prefix = os.environ.get('GCS_OUTPUT_URI_PREFIX')
project_id = os.environ.get('GCP_PROJECT')
location = os.environ.get('PARSER_LOCATION')
processor_id = os.environ.get('PROCESSOR_ID')
geocode_request_topicname = os.environ.get('GEOCODE_REQUEST_TOPICNAME')
kg_request_topicname = os.environ.get('KG_REQUEST_TOPICNAME')
timeout = int(os.environ.get('TIMEOUT'))
 
# An array of Future objects
# Every call to publish() returns an instance of Future
geocode_futures = []
kg_futures = []
# Setting variables
address_substring = "address"
gcs_output_uri = f"gs://{project_id}-output-invoices"
gcs_archive_bucket_name = f"{project_id}-archived-invoices"
destination_uri = f"{gcs_output_uri}/{gcs_output_uri_prefix}/"
name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"
dataset_name = 'invoice_parser_results'
table_name = 'doc_ai_extracted_entities'
 
docai_client = documentai.DocumentProcessorServiceClient()
storage_client = storage.Client()
bq_client = bigquery.Client()
pub_client = pubsub_v1.PublisherClient()
 
def write_to_bq(dataset_name, table_name, entities_extracted_dict):
 
   dataset_ref = bq_client.dataset(dataset_name)
   table_ref = dataset_ref.table(table_name)
   row_to_insert =[]
   row_to_insert.append(entities_extracted_dict)
 
   json_data = json.dumps(row_to_insert, sort_keys=False)
   #Convert to a JSON Object
   json_object = json.loads(json_data)
  
   job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("input_file_name", "STRING"),
        bigquery.SchemaField("carrier", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("currency_exchange_rate", "STRING"),
        bigquery.SchemaField("customer_tax_id", "STRING"),
        bigquery.SchemaField("delivery_date", "DATE"),
        bigquery.SchemaField("due_date", "DATE"),
        bigquery.SchemaField("freight_amount", "STRING"),
        bigquery.SchemaField("invoice_date", "DATE"),
        bigquery.SchemaField("invoice_id", "STRING"),
        bigquery.SchemaField("net_amount", "STRING"),
        bigquery.SchemaField("payment_terms", "STRING"),
        bigquery.SchemaField("purchase_order", "STRING"),
        bigquery.SchemaField("receiver_address", "STRING"),
        bigquery.SchemaField("receiver_email", "STRING"),
        bigquery.SchemaField("receiver_name", "STRING"),
        bigquery.SchemaField("receiver_phone", "STRING"),
        bigquery.SchemaField("receiver_tax_id", "STRING"),
        bigquery.SchemaField("remit_to_address", "STRING"),
        bigquery.SchemaField("remit_to_name", "STRING"),
        bigquery.SchemaField("ship_from_address", "STRING"),
        bigquery.SchemaField("ship_from_name", "STRING"),
        bigquery.SchemaField("ship_to_address", "STRING"),
        bigquery.SchemaField("ship_to_name", "STRING"),
        bigquery.SchemaField("supplier_address", "STRING"),
        bigquery.SchemaField("supplier_email", "STRING"),
        bigquery.SchemaField("supplier_iban", "STRING"),
        bigquery.SchemaField("supplier_name", "STRING"),
        bigquery.SchemaField("supplier_phone", "STRING"),
        bigquery.SchemaField("supplier_registration", "STRING"),
        bigquery.SchemaField("supplier_tax_id", "STRING"),
        bigquery.SchemaField("supplier_website", "STRING"),
        bigquery.SchemaField("total_amount", "STRING"),
        bigquery.SchemaField("total_tax_amount", "STRING"),
        bigquery.SchemaField("vat_amount", "STRING"),
        bigquery.SchemaField("vat_rate", "STRING"),
        bigquery.SchemaField("line_item", "STRING"),
    ])
   job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
 
   job = bq_client.load_table_from_json(json_object, table_ref, job_config=job_config)
   error = job.result()  # Waits for table load to complete.
   print(error)
  
 
def process_invoice(event, context):
   gcs_input_uri = 'gs://' + event['bucket'] + '/' + event['name']
   print('Printing the contentType' + event['contentType'])

   if(event['contentType'] == 'image/gif' or event['contentType'] == 'application/pdf' or event['contentType'] == 'image/tiff' ):
       input_config = documentai.types.document_processor_service.BatchProcessRequest.BatchInputConfig(gcs_source=gcs_input_uri, mime_type=event['contentType'])
       # Where to write results
       output_config = documentai.types.document_processor_service.BatchProcessRequest.BatchOutputConfig(gcs_destination=destination_uri)
 
       request = documentai.types.document_processor_service.BatchProcessRequest(
           name=name,
           input_configs=[input_config],
           output_config=output_config,
       )
 
       operation = docai_client.batch_process_documents(request)
 
       # Wait for the operation to finish
       operation.result(timeout=timeout)
 
       match = re.match(r"gs://([^/]+)/(.+)", destination_uri)
       output_bucket = match.group(1)
       prefix = match.group(2)
      
       #Get a pointer to the GCS bucket where the output will be placed
       bucket = storage_client.get_bucket(output_bucket)
      
       blob_list = list(bucket.list_blobs(prefix=prefix))
       print("Output files:")
 
       for i, blob in enumerate(blob_list):
           # Download the contents of this blob as a bytes object.
           if ".json" not in blob.name:
               print("blob name " + blob.name)
               print(f"skipping non-supported file type {blob.name}")
           else:
                #Setting the output file name based on the input file name
                print("Fetching from " + blob.name)
                start = blob.name.rfind("/") + 1
                end = blob.name.rfind(".") + 1           
                input_filename = blob.name[start:end:] + "gif"
                print("input_filename " + input_filename)
      
                # Getting ready to read the output of the parsed document - setting up "document"
                blob_as_string = blob.download_as_string()
                document = documentai.types.Document.from_json(blob_as_string)
      
                #Reading all entities into a dictionary to write into a BQ table
                entities_extracted_dict = {}
                entities_extracted_dict['input_file_name'] = input_filename
                #entities_extracted = ""
                for entity in document.entities:
                    entity_type = str(entity.type_)
                    if entity_type == 'vat/tax_amount':
                      entity_type = 'vat_amount'
                    if entity_type == 'vat/tax_rate':
                      entity_type = 'vat_rate'
                    #entity_text = str(entity.mention_text)
                    #Normalize date format in case the entity being read is a date
                    if "date" in entity_type:
                      entity_text = entity.normalized_value.text
                      print(entity_text)
                      #d = datetime.strptime(entity_text, '%Y-%m-%d').date()
                      entities_extracted_dict[entity_type] = entity_text
                      #print (d)
                    else:
                      entity_text = str(entity.mention_text)
                      print ("Normalized text : " + entity.normalized_value.text)
                      print ("Mention text : " + entity.mention_text)
                      entities_extracted_dict[entity_type] = entity_text
                      #print(entity_type + ":" + entity_text)



                    # Creating and publishing a message via Pub Sub
                    if (address_substring in entity_type) or entity_type == "supplier_name":
                        print(input_filename)
                        message = {
                        "entity_type": entity_type,
                        "entity_text" : entity_text,
                        "input_file_name": input_filename,
                        }
                        message_data = json.dumps(message).encode("utf-8")
                        
                        if address_substring in entity_type:
                          geocode_topic_path = pub_client.topic_path(project_id,geocode_request_topicname)
                          geocode_future = pub_client.publish(geocode_topic_path, data = message_data)
                          geocode_futures.append(geocode_future)
                        else:
                          kg_topic_path = pub_client.topic_path(project_id,kg_request_topicname)
                          kg_future = pub_client.publish(kg_topic_path, data = message_data)
                          kg_futures.append(kg_future)                 
      
                print(entities_extracted_dict)
                print("Writing to BQ")
                #Write the entities to BQ
                write_to_bq(dataset_name, table_name, entities_extracted_dict)
                
       #print(blobs)
       #Deleting the intermediate files created by the Doc AI Parser
       blobs = bucket.list_blobs(prefix=gcs_output_uri_prefix)
       for blob in blobs:
            blob.delete()
       #Copy input file to archive bucket
       source_bucket = storage_client.bucket(event['bucket'])
       source_blob = source_bucket.blob(event['name'])
       destination_bucket = storage_client.bucket(gcs_archive_bucket_name)
       blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, event['name'])
       #delete from the input folder
       source_blob.delete()
   else:
       print('Cannot parse the file type')

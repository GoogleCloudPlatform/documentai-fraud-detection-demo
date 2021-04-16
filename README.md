
## Objective
Learn how to use Google Cloud Platform to *process and enrich* invoices so that we can **enable fraud detection**.


## Visualizing the flow of data

![diagram](/steps-in-a-diagram.png)

## Services/Tech used in the Demo
* [Google Cloud Document AI](https://cloud.google.com/document-ai)
* [Google Cloud Storage](https://cloud.google.com/storage)
* [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs/overview)
* [Google Cloud Functions](https://cloud.google.com/functions)
* [Cloud Build](https://cloud.google.com/build)
* [Geocoding API](https://developers.google.com/maps/documentation/geocoding/start)
* [Knowledge Graph Search API](https://developers.google.com/knowledge-graph)
* [BigQuery](https://cloud.google.com/bigquery)

## Steps to re-create this demo in your own GCP environment 
1. Create a Google Cloud Platform Project

1. Enable the APIs in the project you created in step #1 above
      * Cloud Document AI API
      * Geocoding API
      * Knowledge Graph Search API
      * Cloud Build API

1. Request access for specialized parsers via [link](https://docs.google.com/forms/d/e/1FAIpQLSc_6s8jsHLZWWE0aSX0bdmk24XDoPiE_oq5enDApLcp1VKJ-Q/viewform?gxids=7826). 
Here is a [link](https://cloud.google.com/document-ai/docs/invoice-parser) to the **official Google Cloud Invoice parser documentation**.

1. Activate your Command Shell and clone this GitHub Repo in your Command shell using the command:
  git clone https://github.com/GoogleCloudPlatform/documentai-fraud-detection-demo.git

1. Execute Bash shell scripts in your Cloud Shell terminal to create cloud resources (i.e Google Cloud Storage Buckets, Pub/Sub topics, Cloud Functions, BigQuery tables)

     1. Change directory to the scripts folder
          * cd scripts

     1. Make all your .sh files executable
          * chmod +x create-output-bucket.sh
          * chmod +x create-archive-bucket.sh
          * chmod +x create-input-bucket.sh
          * chmod +x create-pub-sub-topic.sh
          * chmod +x create-bq-tables.sh
          * chmod +x deploy-cloud-function-process-invoices.sh
          * chmod +x deploy-cloud-function-geocode-addresses.sh
          * chmod +x deploy-cloud-function-get-kg-data.sh

     1. Execute your .sh files to create cloud resources
          * ./create-archive-bucket.sh
          * ./create-input-bucket.sh
          * ./create-output-bucket.sh
          * ./create-pub-sub-topic.sh
          * ./create-bq-tables.sh
          * ./deploy-cloud-function-process-invoices.sh
          * ./deploy-cloud-function-geocode-addresses.sh
          * ./deploy-cloud-function-get-kg-data.sh

     1. Navigate to storage Browser >> <projectid>-output-invoices >> Create an empty folder "processed"

     1. Create your Doc AI processor
          * *Assumption : At this point, your request for access (submitted in Step #3) to the Doc AI specilaized parser has been approved*
          * Go to console > Doc AI > Create processor > Invoice Parser (Under Specilaized)
          * Note the **region** and **ID** of the processor, you will need to plug these values in your cloud function's environment variables
          
     1. [Create an API Key](https://cloud.google.com/docs/authentication/api-keys#creating_an_api_key) - Note the **api key value**, you will need to plug this values in your cloud function's environment variables
     
     1. [Add API restrictions](https://cloud.google.com/docs/authentication/api-keys#adding_api_restrictions) - To set API restrictions:
          1. Select Restrict key in the API restrictions section.
          1. Select Geocoding API, Knowledge Graph Search API that your API key needs to call from the dropdown.
          1. Select the Save button.

1. Edit Environment variables 
     * Edit the Cloud Function geocode-addresses 
          * Update the API_key value under environment variable to match to your API Key 
          * Update the API_key value under .env.yaml to match to your API Key 
     * Edit the Cloud Function get-kg-data
          * Update the API_key value under environment variable to match to your API Key 
          * Update the API_key value under .env.yaml to match to your API Key 
     * Edit the Cloud Function process-invoices
          * Update the PROCESSOR_ID value under environment variable to match to your processor's ID 
          * Update the PROCESSOR_ID value under .env.yaml to match to your processor's ID
          * Update the PARSER_LOCATION value under environment variable to match to your processor's region
          * Update the PARSER_LOCATION value under .env.yaml to match to your processor's region
          
 1. Testing/Validating the demo
     * Upload a sample invoice in the input bucket
     * At the end of the processing, you should expect your BigQuery tables to be populated with extracted entities as well as enriched data (i.e placesID, lat, long, formatted address, name, url, description, kgscore)
     * Reading the results, we can now build custom business intelligenve rules using these enriched fields to enable fraud detection. For example, if the address is not something the Geocoding API can find, then it is an indicator of either incorrect value or fraudulent invoice
     
      



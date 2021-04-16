#! /bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${DIR}/.env.local"

gcloud functions deploy get-kg-data \
--region=${CLOUD_FUNCTION_LOCATION} \
--entry-point=get_kg_data \
--runtime=python38 \
--service-account=${PROJECT_ID}@appspot.gserviceaccount.com \
--source=cloud-functions/get-kg-data \
--timeout=60 \
--env-vars-file=cloud-functions/get-kg-data/.env.yaml \
--trigger-topic=${KG_REQUEST_PUBSUB_TOPIC}

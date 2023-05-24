#!/bin/sh

PROJECT_ID="TO_DO_DEVELOPER"
GCP_REGION="TO_DO_DEVELOPER"
SUBNET="TO_DO_DEVELOPER"
UMSA_FQN="TO_DO_DEVELOPER"
DEPS_BUCKET="TO_DO_DEVELOPER"
BQ_TABLE_FQN="TO_DO_DEVELOPER"

PYSPARK_CODE=${1}

SPARK_CUSTOM_CONTAINER_IMAGE="gcr.io/${PROJECT_ID}/vertex_image:0.1"
gcloud dataproc batches submit pyspark ${PYSPARK_CODE} --region=${GCP_REGION} --deps-bucket=${DEPS_BUCKET}   --subnet=${SUBNET} --service-account=${UMSA_FQN}  --container-image=${SPARK_CUSTOM_CONTAINER_IMAGE} -- --table_fqn=${BQ_TABLE_FQN} --temp_gcs_bucket=${DEPS_BUCKET} 
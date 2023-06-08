#!/bin/sh

LLM_SERVER_ADDRESS="TO_DO_DEVELOPER"
LLM_SERVER_PORT="TO_DO_DEVELOPER"
GCS_FILE_LOCATION="TO_DO_DEVELOPER"
export RAY_ADDRESS="TO_DO_DEVELOPER"
ray job submit --working-dir src/ray --runtime-env-json='{ "pip": ["pyarrow","text-generation","pandas"]}' -- python gen_responses_ray.py --file_name=${GCS_FILE_LOCATION} --server_address=${LLM_SERVER_ADDRESS} --server_port=${LLM_SERVER_PORT} --server_address=${SERVER_ADDRESS} --server_port=${SERVER_PORT}
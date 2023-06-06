#!/bin/sh

FILE_NAME="TO_DO_DEVELOPER"
NUM_SAMPLES="TO_DO_DEVELOPER"
BQ_TABLE_FQN="TO_DO_DEVELOPER"
TEMP_GCS_BUCKET="TO_DO_DEVELOPER"

python3 -m venv local_env
source local_env/bin/activate
pip3 install -r requirements.txt
python3  src/load_data_bq.py --file_name=${FILE_NAME} --num_samples=${NUM_SAMPLES} --bq_table_fqn=${BQ_TABLE_FQN} --temp_gcs_bucket=${TEMP_GCS_BUCKET}
deactivate
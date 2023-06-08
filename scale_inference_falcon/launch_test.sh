#!/bin/sh


PROMPT="TO_DO_DEVELOPER"
SERVER_ADDRESS="TO_DO_DEVELOPER"
SERVER_PORT="TO_DO_DEVELOPER"

python3 -m venv local_env
source local_env/bin/activate
#pip3 install -r requirements.txt
python3  src/test_hf_inference_server.py --prompt="Write a poem about Sevilla" --server_address=${SERVER_ADDRESS} --server_port=${SERVER_PORT} 
deactivate
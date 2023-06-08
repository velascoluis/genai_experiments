from text_generation import Client
import sys
import logging
import argparse
import ray
import pandas as pd
from functools import partial
from datetime import datetime

_LLM_PROMPT_VOUCHER = 'Generate a custom apologies message including offering a discount for the following bad online review: '
_LLM_PROMPT_ACK = 'Generate a custom thank you message for the following positive online review: '
_LLM_PROMPT_NAME = ',  customer name is: '
_LLM_TEMPERATURE=0.4
_LLM_MAX_OUTPUT_TOKENS=128
_LLM_TOP_K=40
_LLM_TOP_P=0.8


def configure_logger():
    """
    Purpose:
        Configure a logger for the script
    Returns:
        Logger object
    """
    logFormatter = logging.Formatter('%(asctime)s - %(filename)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("llm_with_spark")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    logStreamHandler = logging.StreamHandler(sys.stdout)
    logStreamHandler.setFormatter(logFormatter)
    logger.addHandler(logStreamHandler)
    return logger

def parse_args():
    """
    Purpose:
        Parse arguments received by script
    Returns:
        args
    """
    argsParser = argparse.ArgumentParser()
    argsParser.add_argument(
        '--file_name',
        help='File name',
        type=str,
        required=True)
    argsParser.add_argument(
        '--server_address',
        help='Server Address',
        type=str,
        required=True)
    argsParser.add_argument(
        '--server_port',
        help='Server Port',
        type=str,
        required=True)
    
    return argsParser.parse_args()

def gen_response(row,server_address,server_port):
    label = row['label']
    text = row['text']
    name = row['name']
    client = Client(f"http://{server_address}:{server_port}",timeout=30)
    if int(label) < 3:
        prompt = '{} {} {} {}'.format(_LLM_PROMPT_VOUCHER,text,_LLM_PROMPT_NAME,name)
    #Good review
    else:
        prompt = '{} {} {} {}'.format(_LLM_PROMPT_ACK,text,_LLM_PROMPT_NAME,name)  
  
    prompt = prompt.replace('\n', '')
    prompt = prompt.replace('\n\n', '')
    prompt = prompt.replace('  ', ' ')
    prompt = prompt.replace(',',' ')
    prompt = prompt[0:1000]

    response = client.generate(prompt=prompt, max_new_tokens=_LLM_MAX_OUTPUT_TOKENS, top_p=_LLM_TOP_P,top_k=_LLM_TOP_K,temperature=_LLM_TEMPERATURE)
    return {"response": response.generated_text}
   


def exec_ray(logger, args):
    file_name = args.file_name
    server_address = args.server_address
    server_port = args.server_port
    logger.info('Input parameters:')
    logger.info('   * file_name: {}'.format(file_name))
    logger.info('   * server_address: {}'.format(server_address))
    logger.info('   * server_port: {}'.format(server_port))
    try:
        logger.info('Initializing ray ... ')
        ray.init()
        logger.info('Reading  data from GCS ... ')
        input_table_df = ray.data.read_csv(file_name)
        gen_response_map = partial(gen_response, server_address=server_address,server_port=server_port)
        output_table_df = input_table_df.map(gen_response_map)
        print(output_table_df.take_all())


    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed execution!')


if __name__ == "__main__":
    arguments = parse_args()
    logger = configure_logger()
    exec_ray(logger, arguments)




import argparse
import asyncio
import sys
import logging
import itertools
from text_generation import AsyncClient
from google.cloud.bigquery_storage import BigQueryReadClient
from google.cloud.bigquery_storage import types
from google.cloud import bigquery_storage

_LLM_TEMPERATURE=0.4
_LLM_MAX_OUTPUT_TOKENS=128
_LLM_TOP_K=40
_LLM_TOP_P=0.8
_LLM_PROMPT_VOUCHER = 'Generate a custom apologies message including offering a discount for the following bad online review: '
_LLM_PROMPT_ACK = 'Generate a custom thank you message for the following positive online review: '
_LLM_PROMPT_NAME = ',  customer name is: '

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
        '--table_fqn',
        help='Input table',
        type=str,
        required=True)
    argsParser.add_argument(
        '--batch_size',
        help='Batch size',
        type=int,
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


def prep_batch(logger, args):
    table_fqn = args.table_fqn
    logger.info('Input parameters:')
    logger.info('   * table_fqn: {}'.format(table_fqn))
    bqstorageclient = bigquery_storage.BigQueryReadClient()
    project_id = table_fqn.split(".")[0]
    dataset_id = table_fqn.split(".")[1]
    table_id = table_fqn.split(".")[2]
    table = f"projects/{project_id}/datasets/{dataset_id}/tables/{table_id}"
    parent = "projects/{}".format(project_id)
    requested_session = types.ReadSession(table=table,data_format=types.DataFormat.ARROW)
    read_session = bqstorageclient.create_read_session(parent=parent,read_session=requested_session,max_stream_count=4,)
    stream = read_session.streams[0] 
    reader = bqstorageclient.read_rows(stream.name)
    return reader.rows().pages
    

async def run_inference_batch(prompt_batch,logger, args):
    server_address = args.server_address
    server_port = args.server_port
    logger.info('Input parameters:')
    logger.info('   * server_address: {}'.format(server_address))
    logger.info('   * server_port: {}'.format(server_port))
    try:
        client = AsyncClient(f"http://{server_address}:{server_port}")
        tasks = []
        for prompt in prompt_batch:
            tasks.append(client.generate(prompt))
        response =  await asyncio.gather(*tasks)
        print(response)
    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed execution!')


if __name__ == "__main__":
    args = parse_args()
    logger = configure_logger()
    prompt_batch = prep_batch(logger, args)

    






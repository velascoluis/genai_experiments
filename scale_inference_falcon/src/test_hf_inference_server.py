import sys
import logging
import asyncio
import argparse
from datetime import datetime
from text_generation import Client

_LLM_TEMPERATURE=0.35
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
        '--prompt',
        help='LLM Prompt',
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

def test_inference_server(logger, args):
    prompt = args.prompt
    server_address = args.server_address
    server_port = args.server_port
    logger.info('Input parameters:')
    logger.info('   * prompt: {}'.format(prompt))
    logger.info('   * server_address: {}'.format(server_address))
    logger.info('   * server_port: {}'.format(server_port))
    try:
        client = Client(f"http://{server_address}:{server_port}",timeout=30)
        response = client.generate(prompt=prompt, max_new_tokens=_LLM_MAX_OUTPUT_TOKENS, top_p=_LLM_TOP_P,top_k=_LLM_TOP_K,temperature=_LLM_TEMPERATURE)
        print(response.generated_text)
    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed execution!')


if __name__ == "__main__":
    arguments = parse_args()
    logger = configure_logger()
    test_inference_server(logger, arguments)
    
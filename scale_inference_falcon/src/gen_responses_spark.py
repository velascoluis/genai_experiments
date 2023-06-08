from text_generation import Client
import sys
import logging
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf, lit
from pyspark.sql.types import StringType
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
        '--table_fqn',
        help='Input table',
        type=str,
        required=True)
    argsParser.add_argument(
        '--temp_gcs_bucket',
        help='Temp GCS bucket',
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

@udf(returnType=StringType())
def generate_text(label,text,name,server_address,server_port):
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
    return response.generated_text
   

def exec_spark(logger, args):
    table_fqn = args.table_fqn
    temp_gcs_bucket = args.temp_gcs_bucket
    server_address = args.server_address
    server_port = args.server_port
    logger.info('Input parameters:')
    logger.info('   * table_fqn: {}'.format(table_fqn))
    logger.info('   * temp_gcs_bucket: {}'.format(temp_gcs_bucket))
    logger.info('   * server_address: {}'.format(server_address))
    logger.info('   * server_port: {}'.format(server_port))
    try:
        logger.info('Initializing SPARK ... ')
        spark = SparkSession.builder.appName('llm_with_spark').getOrCreate()
        logger.info('Reading source data ...{}'.format(table_fqn))
        input_table_df = spark.read.format('bigquery').option('table', "{}_completed".format(table_fqn)).load()
        output_table_df = input_table_df.withColumn("response", generate_text(col("label"),col("text"),col("name"),lit(server_address),lit(server_port))) 
        output_table_df.write.format("bigquery").mode("overwrite").option("temporaryGcsBucket",temp_gcs_bucket).save("{}_processed".format(table_fqn))
        logger.info('Output results written to ...{}_processed'.format(table_fqn)) 
        spark.stop()
    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed execution!')


if __name__ == "__main__":
    arguments = parse_args()
    logger = configure_logger()
    exec_spark(logger, arguments)




   
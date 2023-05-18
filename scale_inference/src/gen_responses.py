from vertexai.preview.language_models import TextGenerationModel
import sys
import logging
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType
from datetime import datetime

_LLM_PROMPT_VOUCHER = 'Generate a custom apologies message including offering a discount for the following bad online review: '
_LLM_PROMPT_ACK = 'Generate a custom thank yoy message for the following positive online review: '
_LLM_TEMPERATURE=0.2
_LLM_MAX_OUTPUT_TOKENS=256
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
    return argsParser.parse_args()

@udf(returnType=StringType())
def generate_text(input_text, rating):
    model = TextGenerationModel.from_pretrained("text-bison@001")
    #Bad review
    if int(rating) < 3:
        prompt = '{} {}'.format(_LLM_PROMPT_VOUCHER,input_text)
    #Good review
    else:
        prompt = '{} {}'.format(_LLM_PROMPT_ACK,input_text)  
    response = model.predict(
        prompt,
        temperature=_LLM_TEMPERATURE,
        max_output_tokens=_LLM_MAX_OUTPUT_TOKENS,
        top_k=_LLM_TOP_K,
        top_p=_LLM_TOP_P,
    )
    return response.text

def exec_spark(logger, args):
    table_fqn = args.table_fqn
    temp_gcs_bucket = args.temp_gcs_bucket
    logger.info('Input parameters:')
    logger.info('   * table_fqn: {}'.format(table_fqn))
    try:
        logger.info('Initializing SPARK ... ')
        spark = SparkSession.builder.appName('llm_with_spark').getOrCreate()
        logger.info('Reading source data ...{}'.format(table_fqn))
        input_table_df = spark.read.format('bigquery').option('table', table_fqn).load()
        output_table_df = input_table_df.withColumn("response", generate_text(col("text"),col("label"))) 
        output_table_df.write.format("bigquery").option("temporaryGcsBucket",temp_gcs_bucket).save("{}_processed".format(table_fqn))
        logger.info('Output results written to ...{}_processed'.format(table_fqn)) 

    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed execution!')


if __name__ == "__main__":
    arguments = parse_args()
    logger = configure_logger()
    exec_spark(logger, arguments)




   
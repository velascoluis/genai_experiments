from vertexai.preview.language_models import TextGenerationModel
import sys
import logging
import argparse
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import os

_LLM_PROMPT_TABLE = 'Given the following table stored in BigQuery:'
_LLM_PROMPT_CODEGEN = ' generate spark 3.x code using pyspark ,  that reads from BigQuery using  the sentence spark.read.format(bigquery).option(table, REPLACE_FULL_TABLE_NAME_HERE).load(), just replace REPLACE_FULL_TABLE_NAME_HERE with the table name, leave the rest unchanged, to answer the following question:'
_LLM_TEMPERATURE=0.3
_LLM_MAX_OUTPUT_TOKENS=512
_LLM_TOP_K=40
_LLM_TOP_P=0.8
COMMAND="./launch_job.sh"



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
        '--bq_table_full_name',
        help='BQ table name including project name and dataset name',
        type=str,
        required=True)
    argsParser.add_argument(
        '--question',
        help='Question on the data',
        type=str,
        required=True)
    return argsParser.parse_args()

def read_table(logger,bq_table_full_name):
    """
    Purpose:
        Read table from BQ 
    Returns:
        A sample of the table
    """    
    client = bigquery.Client()
    sql = """
    SELECT *
    FROM `{}`
    LIMIT 50
    """.format(bq_table_full_name)
    df = client.query(sql).to_dataframe()
    return df.to_string()

    


def generate_spark_code(logger,input_table_df_injection,question):
    """
    Purpose:
        Generate SPARK code
    Returns:
        SPARK code
    """
    
    model = TextGenerationModel.from_pretrained("text-bison@001")
    prompt = '{} {} {} {}'.format(_LLM_PROMPT_TABLE,input_table_df_injection,_LLM_PROMPT_CODEGEN,question)
    logger.info('Calling LLM with prompt ...{}'.format(prompt))
    response = model.predict(
        prompt,
        temperature=_LLM_TEMPERATURE,
        max_output_tokens=_LLM_MAX_OUTPUT_TOKENS,
        top_k=_LLM_TOP_K,
        top_p=_LLM_TOP_P,
    )

    code = response.text
    formatted_code = code.replace("```", " " )
    return formatted_code

def exec_spark(logger, args):
    bq_table_full_name = args.bq_table_full_name
    question = args.question
    

    logger.info('Input parameters:')
    logger.info('   * bq_table_full_name: {}'.format(bq_table_full_name))
    logger.info('   * question: {}'.format(question))
  
    try:
        logger.info('Initializing execution ... ')
        table = read_table(logger,bq_table_full_name)
        spark_code = generate_spark_code(logger,table,question)
        pyspark_file = open("_pysparkgen.py", "w")
        pyspark_file.write(spark_code)
        pyspark_file.close()
        os.system(COMMAND)

    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed execution!')


if __name__ == "__main__":
    arguments = parse_args()
    logger = configure_logger()
    exec_spark(logger, arguments)




   
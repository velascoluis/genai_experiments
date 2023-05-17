from vertexai.preview.language_models import TextGenerationModel
import sys
import logging
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd
from datetime import datetime

_LLM_PROMPT = 'Explain this table and its key insights:'
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
        '--analysis_number',
        help='Select an analysis, #1 for What are the most popular Citi Bike stations?, \n #2 for What are the most popular routes by subscriber type? \n #3 for What are the top routes by gender?',
        choices=[1, 2, 3],
        type=int,
        required=True)
    argsParser.add_argument(
        '--gen_human_explanation',
        help='Generate a human readable explanation powered by LLM',
        type=bool,
        required=True)
    return argsParser.parse_args()

def generate_insight(output_table_df):
    """
    Purpose:
        Generate a human readable explanation of the analysis using a LLM
    Returns:
        Text with the analysis insights
    """
    output_table_prompt_injection = output_table_df.to_string()
    model = TextGenerationModel.from_pretrained("text-bison@001")
    prompt = '{} {}'.format(_LLM_PROMPT,output_table_prompt_injection)
    response = model.predict(
        prompt,
        temperature=_LLM_TEMPERATURE,
        max_output_tokens=_LLM_MAX_OUTPUT_TOKENS,
        top_k=_LLM_TOP_K,
        top_p=_LLM_TOP_P,
    )

    return response.text

def exec_spark(logger, args):
    gen_human_explanation = args.gen_human_explanation
    analysis_number = args.analysis_number
    input_table_name = 'bigquery-public-data.new_york.citibike_trips'

    logger.info('Input parameters:')
    logger.info('   * analysis_number: {}'.format(analysis_number))
    logger.info('   * gen_human_explanation: {}'.format(gen_human_explanation))
  
    try:

        logger.info('Initializing SPARK ... ')
        spark = SparkSession.builder.appName('llm_with_spark').getOrCreate()
        logger.info('Reading source data ...{}'.format(input_table_name))
        input_table_df = spark.read.format('bigquery').option('table', input_table_name).load()
        logger.info('Executing analysis number ...{}'.format(analysis_number))
        if analysis_number == 1:
            output_table_df = input_table_df.groupBy(['start_station_name','start_station_latitude','start_station_longitude']).agg(F.count('*').alias('num_trips')).select('start_station_name','start_station_latitude','start_station_longitude','num_trips')
            output_table_df = output_table_df.orderBy(F.desc('num_trips')).limit(10).toPandas()
        elif analysis_number == 2:
             output_table_df = input_table_df.groupBy(['start_station_name','end_station_name','usertype']).agg(F.count('*').alias('num_trips'),F.avg(input_table_df.tripduration.cast('int') / 60).alias('duration')).select('usertype',F.concat(input_table_df.start_station_name,input_table_df.end_station_name).alias('route'),'num_trips','duration')
             output_table_df = output_table_df.orderBy(F.desc('num_trips')).limit(10).toPandas()

        else:
            output_table_df = input_table_df.where(input_table_df.gender == "female").where(input_table_df.starttime.cast('string').like('2016%'))
            output_table_df = output_table_df.groupBy(['start_station_name','end_station_name','gender']).agg(F.count('*').alias('num_trips')).select(F.concat(input_table_df.start_station_name,input_table_df.end_station_name).alias('route'),'num_trips','gender')
            output_table_df = output_table_df.orderBy(F.desc('num_trips')).limit(10).toPandas()
        if  gen_human_explanation:
            logger.info('Generating LLM explanation ... ')
            print(generate_insight(output_table_df))
        else:
             logger.info('Generating simple output ... ')
             print(output_table_df.to_string())


    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed execution!')


if __name__ == "__main__":
    arguments = parse_args()
    logger = configure_logger()
    exec_spark(logger, arguments)




   
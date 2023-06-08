import sys
import logging
import argparse
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import  udf
from pyspark.sql.types import StringType
from faker import Factory
from datetime import datetime


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
        required=False)
    argsParser.add_argument(
        '--server_port',
        help='Server Port',
        type=str,
        required=False)    
    return argsParser.parse_args()



@udf(returnType=StringType())
def fake_name():
    """
    Purpose:
        Generate a Fake name
    Returns:
        Returns name
    """
    faker = Factory.create()
    return faker.name()
    

def exec_spark(logger, args):
    table_fqn = args.table_fqn
    temp_gcs_bucket = args.temp_gcs_bucket
    logger.info('Input parameters:')
    logger.info('   * table_fqn: {}'.format(table_fqn))
    logger.info('   * temp_gcs_bucket: {}'.format(temp_gcs_bucket))
    try:
        logger.info('Initializing SPARK ... ')
        spark = SparkSession.builder.appName('llm_with_spark').getOrCreate()
        logger.info('Reading source data ...{}'.format(table_fqn))
        input_table_df = spark.read.format('bigquery').option('table', table_fqn).load()
        output_table_df = input_table_df.withColumn("name", fake_name()) 
        output_table_df.write.format("bigquery").mode("overwrite").option("temporaryGcsBucket",temp_gcs_bucket).save("{}_completed".format(table_fqn))
        logger.info('Output results written to ...{}_completed'.format(table_fqn)) 
        spark.stop()
    except RuntimeError as coreError:
            logger.error(coreError)
    else:
        logger.info('Successfully completed execution!')


if __name__ == "__main__":
    arguments = parse_args()
    logger = configure_logger()
    exec_spark(logger, arguments)
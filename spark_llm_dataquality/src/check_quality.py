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

_LLM_TEMPERATURE=0.2
_LLM_MAX_OUTPUT_TOKENS=256
_LLM_TOP_K=40
_LLM_TOP_P=0.8
_LLM_PROMPT_DQ = """Check if data with multiple fields conform to a standard, answer only with OK if all fields are OK and with FAIL plus the invalid field names and values in case of mismatch
            The values 'Unknown', 'NA, 'Empty' or 'NULL' are always a invalid values.
            The standard rules are:
            [
            "age": A float number,
            "boat": A string,
            "body": An integer,
            "cabin": A string starting with a one ot two letters followed by a number,
            "embarked": An integer between 0 and 3,
            "fare": A float,
            "homedest": A string containing a location,
            "name": A string containing a name,
            "parch": An integer between 0 and 9,
            "pclass": An integer between 0 and 2,
            "sex": An integer between 0 and 1,
            "sibsp": An integer between 0 and 9,
            "ticket": An integer containing the ticket number,
            "survived": An integer between 0 and 1
            ]

             For example, for the following data: [
              "age": "52.0",
              "boat": "4",
              "body": "-1",
              "cabin": "D20",
              "embarked": "0",
              "fare": "78.2667",
              "homedest": "Haverford  PA",
              "name": "Stephenson  Mrs. Walter Bertram (Martha Eustis)",
              "parch": "0",
              "pclass": "0",
              "sex": "1",
              "sibsp": "1",
              "ticket": "36947",
              "survived": "2"
            ]
            The output is output: Survived: 2 is not between 0 and 1

            For example, for the following data: [
              "age": "Pepe",
              "boat": "4",
              "body": "-1",
              "cabin": "D20",
              "embarked": "0",
              "fare": "78.2667",
              "homedest": "Haverford  PA",
              "name": "Stephenson  Mrs. Walter Bertram (Martha Eustis)",
              "parch": "0",
              "pclass": "0",
              "sex": "1",
              "sibsp": "1",
              "ticket": "36947",
              "survived": "2"
            ]
            The output is output: Age: Pepe is not a float

            For example, for the following data: [
              "age": "33.0",
              "boat": "4",
              "body": "-1",
              "cabin": "D20",
              "embarked": "0",
              "fare": "25.0",
              "homedest": "Haverford  PA",
              "name": "Stephenson  Mrs. Walter Bertram (Martha Eustis)",
              "parch": "0",
              "pclass": "0",
              "sex": "1",
              "sibsp": "1",
              "ticket": "36947",
              "survived": "7"
            ]
            The output is output: Homedest: 225.9 is not a string - Survived: 7 is not between 0 and 1

            For example, for the following data: [
              "age": "33.0",
              "boat": "4",
              "body": "-1",
              "cabin": "Cabin17",
              "embarked": "0",
              "fare": "25.0",
              "homedest": "Haverford  PA",
              "name": "Stephenson  Mrs. Walter Bertram (Martha Eustis)",
              "parch": "0",
              "pclass": "0",
              "sex": "9",
              "sibsp": "1",
              "ticket": "36947",
              "survived": "0"
            ]
            The output is output: cabin: Cabin17 is not a string starting with a one or two letters followed by a number - sex: 9 is not a string starting with a one or two letters followed by a number
              For example, for the following data: [
              "age": "33.0",
              "boat": "Unknown",
              "body": "-1",
              "cabin": "D20",
              "embarked": "0",
              "fare": "25.0",
              "homedest": "Haverford  PA",
              "name": "Stephenson  Mrs. Walter Bertram (Martha Eustis)",
              "parch": "0",
              "pclass": "0",
              "sex": "Unknown",
              "sibsp": "1",
              "ticket": "36947",
              "survived": "7"
            ]
            The is output output: boat: Unknown is not valid
            sex: Unknown is not valid
            Survived: 7 is not between 0 and 1
        
            The actual data to try is the following:
            [
              "age": {},
              "boat": {},
              "body": {},
              "cabin": {},
              "embarked": {},
              "fare": {},
              "homedest": {},
              "name": {},
              "parch": {},
              "pclass": {},
              "sex": {},
              "sibsp": {},
              "ticket": {},
              "survived": {}
            ]  
    """




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
def check_quality(age,boat,body,cabin,embarked,fare,homedest,name,parch,pclass,sex,sibsp,ticket,survived):
    model = TextGenerationModel.from_pretrained("text-bison@001")
    args = (age,boat,body,cabin,embarked,fare,homedest,name,parch,pclass,sex,sibsp,ticket,survived)
    args_str = list(map(str, args))
    args_stp = list(map(str.strip, args_str))
     
    prompt = _LLM_PROMPT_DQ.format(*args)
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
        input_table_df = spark.read.format('bigquery').option('table', "{}".format(table_fqn)).load()
        output_table_df = input_table_df.withColumn("quality", check_quality(input_table_df.age,input_table_df.boat,input_table_df.body,input_table_df.cabin,input_table_df.embarked,input_table_df.fare,input_table_df.homedest,input_table_df.name,input_table_df.parch,input_table_df.pclass,input_table_df.sex,input_table_df.sibsp,input_table_df.ticket,input_table_df.survived)) 
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





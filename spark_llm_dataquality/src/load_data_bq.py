import tensorflow as tf
import tensorflow_datasets as tfds
from google.cloud import bigquery
from google.cloud import storage
import argparse


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
        help='Staging file name',
        type=str,
        required=True)
    argsParser.add_argument(
        '--num_samples',
        help='Num samples to extract',
        type=int,
        required=True)
    argsParser.add_argument(
        '--bq_table_fqn',
        help='BigQuery table to load',
        type=str,
        required=True)
    argsParser.add_argument(
        '--temp_gcs_bucket',
        help='Temo GCS bucket',
        type=str,
        required=True)
    return argsParser.parse_args()


def upload_file_gcs(gcs_bucket,file_name):
    client = storage.Client()
    bucket = client.get_bucket(gcs_bucket)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)

def load_bq(file_uri,table_fqn):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("age", "FLOAT64"),
        bigquery.SchemaField("boat", "STRING"),
        bigquery.SchemaField("body", "INT64"),
        bigquery.SchemaField("cabin", "STRING"),
        bigquery.SchemaField("embarked", "INT64"),
        bigquery.SchemaField("fare", "FLOAT64"),
        bigquery.SchemaField("homedest", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("parch", "INT64"),
        bigquery.SchemaField("pclass", "INT64"),
        bigquery.SchemaField("sex", "INT64"),
        bigquery.SchemaField("sibsp", "INT64"),
        bigquery.SchemaField("ticket", "STRING"),
        bigquery.SchemaField("survived", "INT64")
    ],
    source_format=bigquery.SourceFormat.CSV,
    )
    load_job = client.load_table_from_uri(file_uri, table_fqn, job_config=job_config)  
    load_job.result()  
    destination_table = client.get_table(table_fqn) 
    print("Loaded {} rows.".format(destination_table.num_rows))

def extract_to_csv(file_name,num_samples):
    ds = tfds.load('titanic')
    ds = ds['train']
    with open(file_name, 'w') as datafile:
        for example in ds.take(num_samples):
            age = str(example['age'].numpy())
            boat = str(example['boat'].numpy().decode('UTF-8'))
            body = str(example['body'].numpy())
            cabin= str(example['cabin'].numpy().decode('UTF-8'))
            embarked= str(example['embarked'].numpy()).replace(",", " " )
            fare= str(example['fare'].numpy())
            homedest= str(example['home.dest'].numpy().decode('UTF-8')).replace(",", " " )
            name = str(example['name'].numpy().decode('UTF-8')).replace(",", " " )
            parch = str(example['parch'].numpy())
            pclass = str(example['pclass'].numpy())
            sex = str(example['sex'].numpy())
            sibsp = str(example['sibsp'].numpy())
            ticket = str(example['ticket'].numpy().decode('UTF-8'))
            survived = str(example['survived'].numpy())
            datafile.write('{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n'.format(age,boat,body,cabin,embarked,fare,homedest,name,parch,pclass,sex,sibsp,ticket,survived))


if __name__ == "__main__":
    args = parse_args()
    file_name = args.file_name
    num_samples = args.num_samples
    bq_table_fqn = args.bq_table_fqn
    temp_gcs_bucket = args.temp_gcs_bucket
    extract_to_csv(file_name,num_samples)
    upload_file_gcs(temp_gcs_bucket,file_name)
    load_bq('gs://{}/{}'.format(temp_gcs_bucket,file_name),bq_table_fqn)
    
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
        bigquery.SchemaField("label", "STRING"),
        bigquery.SchemaField("text", "STRING"),
    ],
    source_format=bigquery.SourceFormat.CSV,
    )
    load_job = client.load_table_from_uri(file_uri, table_fqn, job_config=job_config)  # Make an API request.
    load_job.result()  
    destination_table = client.get_table(table_fqn) 
    print("Loaded {} rows.".format(destination_table.num_rows))

def extract_to_csv(file_name,num_samples):
    ds = tfds.load('huggingface:yelp_review_full/yelp_review_full')
    ds = ds['train']
    with open(file_name, 'w') as datafile:
        for example in ds.take(num_samples):
            text, label = example["text"], example["label"]
            text = (str(text.numpy().decode('UTF-8'))).replace(",", " " )
            label = (str(label.numpy()))
            datafile.write('{},{}\n'.format(label,text))


if __name__ == "__main__":
    args = parse_args()
    file_name = args.file_name
    num_samples = args.num_samples
    bq_table_fqn = args.bq_table_fqn
    temp_gcs_bucket = args.temp_gcs_bucket
    extract_to_csv(file_name,num_samples)
    upload_file_gcs(temp_gcs_bucket,file_name)
    load_bq('gs://{}/{}'.format(temp_gcs_bucket,file_name),bq_table_fqn)
    
# Vertex Generative AI inference via dataproc serverless

This repository contains an example that demonstrate how to run
[Generative AI](https://cloud.google.com/ai/generative-ai), with few shot learning powered by [Vertex AI](https://cloud.google.com/vertex-ai) via python SPARK UDFs using 
 [dataproc serverless SPARK]( https://cloud.google.com/dataproc-serverless)  


## Use case
The example uses the [Titanic dataset](https://www.tensorflow.org/datasets/catalog/titanic), a dataset describing the survival status of individual passengers on the Titanic. The objective is to perform a data quality check using LLM inference.

## Sample outputs

**Prompt design using Vertex AI Generative Studio :**

![Sample output 0](assets/00.png)

**Augmented dataset with LLM output:**

![Sample output 1](assets/01.png)

NOTE: [Hallucination](https://en.wikipedia.org/wiki/Hallucination_(artificial_intelligence)) will likely occur

## Running the code

1. Select or create a Google Cloud project, and enable the required APIs.

2. Create a service account with enough permissions to interact with the different services (BigQuery, dataproc spark, Vertex AI).

3. Open `Cloud shell` and clone this repository

4. Edit the `build_dataproc_image.sh` file and specify:
```bash
PROJECT_ID="TO_DO_DEVELOPER"
GCP_REGION="TO_DO_DEVELOPER"
```
5. Build a custom dataproc image that contains the Vertex AI python SDK
```bash
build_dataproc_image.sh
```
6. Edit the `launch_bq_loader.sh` file and specify:

```bash
FILE_NAME="TO_DO_DEVELOPER"
NUM_SAMPLES="TO_DO_DEVELOPER"
BQ_TABLE_FQN="TO_DO_DEVELOPER"
TEMP_GCS_BUCKET="TO_DO_DEVELOPER"
```

7. Load data into BigQuery:

```bash
launch_bq_loader.sh
```

8. Edit the `launch_job.sh` file and specify:

```bash
PROJECT_ID="TO_DO_DEVELOPER"
GCP_REGION="TO_DO_DEVELOPER"
SUBNET="TO_DO_DEVELOPER"
UMSA_FQN="TO_DO_DEVELOPER"
DEPS_BUCKET="TO_DO_DEVELOPER"
BQ_TABLE_FQN="TO_DO_DEVELOPER"
```

9. Launch the dataproc serverless job
```bash
launch_job.sh src/check_quality.py
```
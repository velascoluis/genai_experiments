#!/bin/sh

PROJECT_ID="TO_DO_DEVELOPER"
GCP_REGION="TO_DO_DEVELOPER"

LOCAL_SCRATCH_DIR=~/build
DOCKER_IMAGE_TAG="0.1"
DOCKER_IMAGE_NM="vertex_image"
DOCKER_IMAGE_FQN="gcr.io/$PROJECT_ID/$DOCKER_IMAGE_NM:$DOCKER_IMAGE_TAG"
BQ_CONNECTOR_JAR_URI="gs://spark-lib/bigquery/spark-3.3-bigquery-0.30.0.jar"


# Create local directory
cd ~
mkdir build
cd build
rm -rf *
echo "Created local directory for the Docker image building"

# Create Dockerfile in local directory
cd $LOCAL_SCRATCH_DIR

cat << 'EOF' > Dockerfile
# Debian 11 is recommended.
FROM debian:11-slim
# Suppress interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
# (Required) Install utilities required by Spark scripts.
RUN apt update && apt install -y procps tini
# (Optional) Add extra jars.
# Debian 11 is recommended.
FROM debian:11-slim
# Suppress interactive prompts
ENV DEBIAN_FRONTEND=noninteractive
# (Required) Install utilities required by Spark scripts.
RUN apt update && apt install -y procps tini
# (Optional) Add extra jars.
ENV SPARK_EXTRA_JARS_DIR=/opt/spark/jars/
ENV SPARK_EXTRA_CLASSPATH='/opt/spark/jars/*'
RUN mkdir -p "${SPARK_EXTRA_JARS_DIR}"
COPY "${BQ_CONNECTOR_JAR_URI}" "${SPARK_EXTRA_JARS_DIR}"
# (Optional) Install and configure Miniconda3.
ENV CONDA_HOME=/opt/miniconda3
ENV PYSPARK_PYTHON=${CONDA_HOME}/bin/python
ENV PATH=${CONDA_HOME}/bin:${PATH}
COPY Miniconda3-py39_4.10.3-Linux-x86_64.sh .
RUN bash Miniconda3-py39_4.10.3-Linux-x86_64.sh -b -p /opt/miniconda3 \
  && ${CONDA_HOME}/bin/conda config --system --set always_yes True \
  && ${CONDA_HOME}/bin/conda config --system --set auto_update_conda False \
  && ${CONDA_HOME}/bin/conda config --system --prepend channels conda-forge \
  && ${CONDA_HOME}/bin/conda config --system --set channel_priority strict
# (Optional) Install Conda packages.
#
# The following packages are installed in the default image, it is strongly
# recommended to include all of them.
#
# Use mamba to install packages quickly.
RUN ${CONDA_HOME}/bin/conda install mamba -n base -c conda-forge \
    && ${CONDA_HOME}/bin/mamba install \
      conda \
      cython \
      fastavro \
      fastparquet \
      faker \
      gcsfs \
      google-cloud-bigquery-storage \
      google-cloud-bigquery[pandas] \
      google-cloud-bigtable \
      google-cloud-container \
      google-cloud-datacatalog \
      google-cloud-dataproc \
      google-cloud-datastore \
      google-cloud-language \
      google-cloud-logging \
      google-cloud-monitoring \
      google-cloud-pubsub \
      google-cloud-redis \
      google-cloud-spanner \
      google-cloud-speech \
      google-cloud-storage \
      google-cloud-texttospeech \
      google-cloud-translate \
      google-cloud-vision \
      google-cloud-aiplatform \
      koalas \
      matplotlib \
      mleap \
      nltk \
      numba \
      numpy \
      openblas \
      orc \
      pandas \
      pip \
      pyarrow \
      pysal \
      pytables \
      python \
      regex \
      requests \
      rtree \
      scikit-image \
      scikit-learn \
      scipy \
      seaborn \
      sqlalchemy \
      sympy \
      virtualenv
RUN pip install text-generation
# (Optional) Install R and R libraries.
RUN apt update \
  && apt install -y gnupg \
  && apt-key adv --no-tty \
      --keyserver "hkp://keyserver.ubuntu.com:80" \
      --recv-keys 95C0FAF38DB3CCAD0C080A7BDC78B2DDEABC47B7 \
  && echo "deb http://cloud.r-project.org/bin/linux/debian bullseye-cran40/" \
      >/etc/apt/sources.list.d/cran-r.list \
  && apt update \
  && apt install -y \
      libopenblas-base \
      libssl-dev \
      r-base \
      r-base-dev \
      r-recommended \
      r-cran-blob
ENV R_HOME=/usr/lib/R
# (Required) Create the 'spark' group/user.
# The GID and UID must be 1099. Home directory is required.
RUN groupadd -g 1099 spark
RUN useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark
EOF

echo "Completed Dockerfile creation"

# Download dependencies to be baked into image
cd $LOCAL_SCRATCH_DIR
gsutil cp $BQ_CONNECTOR_JAR_URI .
wget -P . https://repo.anaconda.com/miniconda/Miniconda3-py39_4.10.3-Linux-x86_64.sh
echo "Completed downloading dependencies"

# Authenticate 
gcloud auth configure-docker ${GCP_REGION}-docker.pkg.dev -q

# Build image
docker build . --progress=tty -f Dockerfile -t $DOCKER_IMAGE_FQN


echo "Completed docker image build"

# Push to GCR
docker push $DOCKER_IMAGE_FQN
echo "Completed docker image push to GCR"
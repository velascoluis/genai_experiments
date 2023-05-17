python3 -m venv local_env
source local_env/bin/activate
pip3 install -r requirements.txt
python3  src/code_gen.py --bq_table_full_name="bigquery-public-data.new_york.citibike_trips" --question="What are the most popular Citi Bike stations?"
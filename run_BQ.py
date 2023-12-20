import logging
from google.cloud import bigquery

# Set up logging configuration
logging.basicConfig(filename='logs/bigquery_query.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Authenticate with your service account key JSON file
client = bigquery.Client.from_service_account_json('credentials.json')  # Replace with your credentials path

dataset_id = 'dataeng2-407917.dataset_dataeng2'  # Replace with your dataset ID in BigQuery
table_id = 'table_all_products'  # Replace with the table name

# Construct a reference to the table
table_ref = client.dataset(dataset_id).table(table_id)

# Write your SQL query
query = """
CREATE OR REPLACE TABLE dataeng2-407917.dataset_dataeng2.table_iphone AS
SELECT *
FROM dataeng2-407917.dataset_dataeng2.table_all_products
WHERE Comment_Subreddit = 'iphone15';

CREATE OR REPLACE TABLE dataeng2-407917.dataset_dataeng2.table_samsung AS
SELECT *
FROM dataeng2-407917.dataset_dataeng2.table_all_products
WHERE Comment_Subreddit = 'GalaxyS23';

"""

try:
    # Run the query
    query_job = client.query(query)

    # Wait for the query to complete and fetch the results
    results = query_job.result()
    logging.info("Successfully ran the query")
    print("Successfully ran the query")

except Exception as e:
    logging.error("Error occurred: %s", str(e))
    print("An error occurred:", e)

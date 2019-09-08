from google.cloud import bigquery
import uuid
import os
import time
import datetime
os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")


def handle_errors(job):
    client = bigquery.Client()
    errors = job.errors
    errors[0]['job_id'] = job.job_id
    errors[0]['event_time'] = datetime.datetime.utcnow()
    dataset_id = 'analysts_playground'
    table_id = 'logs'
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)  # API request
    
    schema = table.schema
    row = [tuple([
        errors[0][field.name] if field.name in errors[0].keys() else '' for field in table.schema
    ])]
    errors = client.insert_rows(table, row)
    print("{} rows inserted to {}_{}".format(len(row),dataset_id,table_id))
    return False

def task(query,return_df=False,write_disposition='WRITE_APPEND ',*table_args):
    client = bigquery.Client()
    job_config = bigquery.job.QueryJobConfig()
    query_job = bigquery.job.QueryJob(str(uuid.uuid4()),
        query, client=client, job_config=job_config)
    query_job._begin()
    while not query_job.done():
        time.sleep(5)
    try:
        result = query_job.result()
        errors = query_job.errors
        if not errors:
            print('Done Successfully')
            if return_df:
                return result.to_dataframe()
            else:
                return True
   
    except:
        return handle_errors(query_job)
        

import pandas as pd
from google.cloud import bigquery
import uuid
import os
from utils.get_dates import get_date
import time
os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")


def load_from_df(df,dataset='analysts_playground',table='sessions',write_disposition='WRITE_TRUNCATE'):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    job_config = bigquery.job.LoadJobConfig( autodetect=False, write_disposition='WRITE_TRUNCATE' )
    job_id = str(uuid.uuid4())
    job = client.load_table_from_dataframe(df, table_ref,num_retries=3,job_id=job_id,job_id_prefix='sessions_{}'.format(get_date),job_config=job_config)
    while not job.done():
        time.sleep(5)
    print('Job is Done?',job.done())
    table = client.get_table(table_ref)
    print('NUmber Of rows in BQ',table.num_rows)
    print('Number of rows in DF',df.shape[0])
    
    if  table.num_rows == df.shape[0]:
        return True, job
    else:
        return False, job

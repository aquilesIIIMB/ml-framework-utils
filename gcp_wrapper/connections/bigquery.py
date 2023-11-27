import os
import platform
import pyarrow as pa
import pyarrow.parquet as pq
import gcsfs
import dask.dataframe as dd
import google.auth
import google.auth.transport.requests
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import bigquery_storage  
try:
    from mlutils import connector
except:
    pass


def modify_query_file(input_name: str, replacements: dict={}, path='../scripts/queries/'):
    """
    Modifies file in place with a dictionary of string replacements
    """
    with open(path+input_name, 'r') as file :
        filedata = file.read()
    if replacements:
        for key, value in replacements.items():
            filedata = filedata.replace(key, value)
    return filedata

def modify_query(input_query: str, replacements: dict={}):
    """
    Modifies file in place with a dictionary of string replacements
    """
    if replacements:
        for key, value in replacements.items():
            input_query = input_query.replace(key, value)
    return input_query


class BigQuery():
    def __init__(self, key_path):
        if platform.system() == 'Linux':
            if key_path.find('.json') != -1:
                self.client = bigquery.Client.from_service_account_json(key_path)  
            elif key_path:
                self.client = connector.get_connector(name=key_path)
            else:
                self.client = bigquery.Client()

            self.project_id = self.client.project
            client_bq_ext=bigquery.client.Connection(self.client)
            self.credentials = client_bq_ext.credentials
            self.client_bq_storage=bigquery_storage.BigQueryReadClient(credentials=self.credentials)
            auth_req = google.auth.transport.requests.Request()
            self.credentials.refresh(auth_req)
        else:
            self.credentials = service_account.Credentials.from_service_account_file(
                key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self.client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id,)

            self.project_id = self.credentials.project_id
            self.client_bq_storage=bigquery_storage.BigQueryReadClient(credentials=self.credentials)
            auth_req = google.auth.transport.requests.Request()
            self.credentials.refresh(auth_req)

    def execute(self, query, params:dict={}):
        # write_disposition: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        query_statement=modify_query(query, params)
        job_config = bigquery.QueryJobConfig(allow_large_results=True, use_legacy_sql=False, use_query_cache=True) 
        self.query=self.client.query(query_statement, job_config=job_config)
        self.query.result()

    def query2df(self, query, params:dict={}):
        query_statement=modify_query(query, params)
        job_config = bigquery.QueryJobConfig(allow_large_results=True, use_legacy_sql=False, use_query_cache=True)
        self.query=self.client.query(query_statement, job_config=job_config).to_arrow(bqstorage_client=self.client_bq_storage)
        return self.query.to_pandas()
    
    def query2table(self, query, params:dict={}, write_disposition='WRITE_APPEND', table_dest='output', dataset_dest='tmp', project_id_dest=None):
        # write_disposition: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        # ref: https://cloud.google.com/bigquery/docs/writing-results
        project_id_dest = project_id_dest if project_id_dest else self.project_id
        query_statement=modify_query(query, params)
        
        job_config = bigquery.QueryJobConfig(destination=f'{project_id_dest}.{dataset_dest}.{table_dest}', allow_large_results=True, use_legacy_sql=False, use_query_cache=True, write_disposition=write_disposition) 
        self.query=self.client.query(query_statement, job_config=job_config)
        self.query.result()
        
        table_id = self.client.get_table(f'{project_id_dest}.{dataset_dest}.{table_dest}') # Make an API request.
        print("Table {}: {} rows and {} columns".format(table_dest, table_id.num_rows, len(table_id.schema)))
    
    def execute_file(self, query_name, params:dict={}, path='../scripts/queries/'):
        # write_disposition: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        query_statement=modify_query_file(query_name, params, path)
        job_config = bigquery.QueryJobConfig(allow_large_results=True, use_legacy_sql=False, use_query_cache=True) 
        self.query=self.client.query(query_statement, job_config=job_config)
        self.query.result()

    def queryfile2df(self, query_name, params:dict={}, path='../scripts/queries/'):
        query_statement=modify_query_file(query_name, params, path)
        job_config = bigquery.QueryJobConfig(allow_large_results=True, use_legacy_sql=False, use_query_cache=True)
        self.query=self.client.query(query_statement, job_config=job_config).to_arrow(bqstorage_client=self.client_bq_storage)
        return self.query.to_pandas()

    def queryfile2table(self, query_name, params:dict={}, path='../scripts/queries/', write_disposition='WRITE_APPEND', table_dest='output', dataset_dest='tmp', project_id_dest=None):
        # write_disposition: WRITE_TRUNCATE, WRITE_APPEND, WRITE_EMPTY
        # ref: https://cloud.google.com/bigquery/docs/writing-results
        project_id_dest = project_id_dest if project_id_dest else self.project_id
        query_statement=modify_query_file(query_name, params, path)
        
        job_config = bigquery.QueryJobConfig(destination=f'{project_id_dest}.{dataset_dest}.{table_dest}', allow_large_results=True, use_legacy_sql=False, use_query_cache=True, write_disposition=write_disposition) 
        self.query=self.client.query(query_statement, job_config=job_config)
        self.query.result()
        
        table_id = self.client.get_table(f'{project_id_dest}.{dataset_dest}.{table_dest}') # Make an API request.
        print("Table {}: {} rows and {} columns".format(table_dest, table_id.num_rows, len(table_id.schema)))

    def storage2table(self, bucket_input_path, schema_file_path=None, table_dest='output', dataset_dest='tmp', project_id_dest=None):
        project_id_dest = project_id_dest if project_id_dest else self.project_id
        # ref: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#bq
        if schema_file_path:
            output_sys=os.system(f'bq load --source_format={bucket_input_path.split(".")[-1].upper()} {project_id_dest}:{dataset_dest}.{table_dest} {bucket_input_path} {schema_file_path}')
        else:
            output_sys=os.system(f'bq load --autodetect --source_format={bucket_input_path.split(".")[-1].upper()} {project_id_dest}:{dataset_dest}.{table_dest} {bucket_input_path}')            

        return output_sys

    def df2table(self, dataframe, table_schema=None, table_dest='output', dataset_dest='tmp', project_id_dest=None, if_exists='append'): 
        # cliente_bigquery.load_table_from_dataframe
        project_id_dest = project_id_dest if project_id_dest else self.project_id
        table_id = f'{dataset_dest}.{table_dest}'       
        dataframe.to_gbq(table_id, project_id=project_id_dest, table_schema=table_schema, if_exists=if_exists, credentials=self.credentials)

        table_id = self.client.get_table(f'{project_id_dest}.{dataset_dest}.{table_dest}') # Make an API request.
        print("Table {}: {} rows and {} columns".format(table_dest, table_id.num_rows, len(table_id.schema)))   
        

    def table2storage(self, table_origin, dataset_origin, project_id_origin=None, bucket_output_path=None):
        # ref: https://cloud.google.com/bigquery/docs/exporting-data#bq
        project_id_origin = project_id_origin if project_id_origin else self.project_id
        bucket_output_path = bucket_output_path if bucket_output_path else f'gs://{self.project_id}/table_output_*.parquet'
        dataset_ref = bigquery.DatasetReference(project_id_origin, dataset_origin)
        table_ref = dataset_ref.table(table_origin)

        if bucket_output_path.split(".")[-1]=='csv':
            job_config = bigquery.job.ExtractJobConfig()
            job_config.compression = bigquery.Compression.GZIP
            job_config.destination_format = bigquery.DestinationFormat.CSV

            extract_job = self.client.extract_table(
                table_ref,
                bucket_output_path,
                # Location must match that of the source table.
                location="US",
                job_config=job_config,
            )  # API request
            extract_job.result()  # Waits for job to complete.

            print("Exported {}:{}.{} to {}".format(project_id_origin, dataset_origin, table_origin, bucket_output_path))
            #output_sys=os.system(f'bq extract --compression GZIP {project_id_origin}:{dataset_origin}.{table_origin} {bucket_output_path}')
        elif bucket_output_path.split(".")[-1]=='json':
            job_config = bigquery.job.ExtractJobConfig()
            job_config.compression = bigquery.Compression.GZIP
            job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON

            extract_job = self.client.extract_table(
                table_ref,
                bucket_output_path,
                # Location must match that of the source table.
                location="US",
                job_config=job_config,
            )  # API request
            extract_job.result()  # Waits for job to complete.

            print("Exported {}:{}.{} to {}".format(project_id_origin, dataset_origin, table_origin, bucket_output_path))
            #output_sys=os.system(f'bq extract --compression GZIP --destination_format NEWLINE_DELIMITED_JSON {project_id_origin}:{dataset_origin}.{table_origin} {bucket_output_path}')
        elif bucket_output_path.split(".")[-1]=='avro':
            job_config = bigquery.job.ExtractJobConfig()
            job_config.compression = bigquery.Compression.SNAPPY
            job_config.destination_format = bigquery.DestinationFormat.AVRO

            extract_job = self.client.extract_table(
                table_ref,
                bucket_output_path,
                # Location must match that of the source table.
                location="US",
                job_config=job_config,
            )  # API request
            extract_job.result()  # Waits for job to complete.

            print("Exported {}:{}.{} to {}".format(project_id_origin, dataset_origin, table_origin, bucket_output_path))
            #output_sys=os.system(f'bq extract --destination_format AVRO --compression SNAPPY {project_id_origin}:{dataset_origin}.{table_origin} {bucket_output_path}')
        elif bucket_output_path.split(".")[-1]=='parquet':
            job_config = bigquery.job.ExtractJobConfig()
            job_config.compression = bigquery.Compression.SNAPPY
            job_config.destination_format = bigquery.DestinationFormat.PARQUET

            extract_job = self.client.extract_table(
                table_ref,
                bucket_output_path,
                # Location must match that of the source table.
                location="US",
                job_config=job_config,
            )  # API request
            extract_job.result()  # Waits for job to complete.

            print("Exported {}:{}.{} to {}".format(project_id_origin, dataset_origin, table_origin, bucket_output_path))
            #output_sys=os.system(f'bq extract --destination_format PARQUET --compression SNAPPY {project_id_origin}:{dataset_origin}.{table_origin} {bucket_output_path}')
        else:
            raise ValueError('please use a valid format', 'csv', 'json', 'avro', 'parquet')
        return extract_job

    def list_tables(self, dataset, project_id=None):
        project_id = project_id if project_id else self.project_id
        output_sys=os.system(f'bq ls --format=pretty --max_results integer {project_id}:{dataset}')
        print('Tables:', output_sys)
        for table_bq in self.client.list_tables(f'{project_id}.{dataset}'):
            print(table_bq.table_id)
            
    def get_table_info(self, table_dest='output', dataset_dest='tmp', project_id_dest=None):
        project_id_dest = project_id_dest if project_id_dest else self.project_id
        table_id = self.client.get_table(f'{project_id_dest}.{dataset_dest}.{table_dest}') # Make an API request.
        print("Table {}: {} rows and {} columns".format(table_dest, table_id.num_rows, len(table_id.schema)))
        return table_id

    def create_dataset(self, dataset_name):
        try:
            self.client.create_dataset(dataset_name)
        except:
            pass

    def delete_dataset(self, dataset_name, delete_contents=False):
        try:
            self.client.delete_dataset(dataset_name, delete_contents=delete_contents)
        except:
            pass

    def create_table(self, table_name):
        try:
            self.client.create_table(table_name)
        except:
            pass

    def delete_table(self, table_name):
        try:
            self.client.delete_table(table_name)
        except:
            pass

    def close(self):
        self.client.close()


if __name__ == "__main__":
    pass

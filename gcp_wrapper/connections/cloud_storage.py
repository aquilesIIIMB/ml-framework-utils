import os
import re
import platform
import pyarrow as pa
import pyarrow.parquet as pq
import gcsfs
import dask.dataframe as dd
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account
from google.cloud import storage
try:
    from mlutils import connector
except:
    pass


### Leer archivos de Bucket
### Enlistar buckets disponibles 
### Poder descargar varios archivos a la vez
### Poder subir varios archivos a la vez


class CloudStorage():
    def __init__(self, key_path):
        if platform.system() == 'Linux':
            if key_path.find('.json') != -1:
                self.client = storage.Client.from_service_account_json(key_path)
            elif key_path:
                self.client = connector.get_connector(name=key_path)
            else:
                self.client = storage.Client()
            
            self.project_id = self.client.project
            client_storage_ext=storage.client.Connection(self.client)
            self.credentials = client_storage_ext.credentials
            auth_req = google.auth.transport.requests.Request()
            self.credentials.refresh(auth_req)
            self.fs = gcsfs.GCSFileSystem(project=self.client.project, token=self.credentials)
        else:
            self.credentials = service_account.Credentials.from_service_account_file(
                key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self.client = storage.Client(credentials=self.credentials, project=self.credentials.project_id,)
            
            self.project_id = self.client.project
            client_storage_ext=storage.client.Connection(self.client)
            self.credentials = client_storage_ext.credentials
            auth_req = google.auth.transport.requests.Request()
            self.credentials.refresh(auth_req)
            self.fs = gcsfs.GCSFileSystem(project=self.client.project, token=self.credentials)


    def storage2table(self, bucket_input_path, schema_file_path=None, table_dest='output', dataset_dest='tmp', project_id_dest=None):
        project_id_dest = project_id_dest if project_id_dest else self.project_id
        # ref: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-csv#bq
        if schema_file_path:
            output_sys=os.system(f'bq load --source_format={bucket_input_path.split(".")[-1].upper()} {project_id_dest}:{dataset_dest}.{table_dest} {bucket_input_path} {schema_file_path}')
        else:
            output_sys=os.system(f'bq load --autodetect --source_format={bucket_input_path.split(".")[-1].upper()} {project_id_dest}:{dataset_dest}.{table_dest} {bucket_input_path}')            

        return output_sys
        

    def df2storage(self, dataframe, file_type='parquet', bucket_output_path=None): 
        # ref: https://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.Series.to_csv
        bucket_output_path = bucket_output_path if bucket_output_path else f'gs://{self.project_id}/table_output'
        dask_df = dd.from_pandas(dataframe, chunksize=1000000) 
        
        if file_type=='parquet':
            output_gcs = dask_df.to_parquet(bucket_output_path, engine='pyarrow', schema="infer", write_index=False, write_metadata_file=False, compression="snappy", storage_options={'project':self.project_id, 'token':self.credentials}) 
        elif file_type=='csv':
            output_gcs = dask_df.to_csv(bucket_output_path+"/part_*.csv", index=False, compression='gzip', storage_options={'project':self.project_id, 'token':self.credentials})
        elif file_type=='json':
            output_gcs = dask_df.to_json(bucket_output_path+"/part_*.json", compression='gzip', storage_options={'project':self.project_id, 'token':self.credentials})
        else:
            raise ValueError('please use a valid format', 'csv', 'json', 'avro', 'parquet')    
        

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

    def download_files(self, bucket_name, bucket_path, local_destination_path):
        #bucket_name = 'model-training-aa8eedc6-d191-4f82-8085-94ef9031cf29'
        #bucket_path = 'tutorials/model-8354960161051246592/tf-saved-model/2021-08-01T16:14:13.897231Z/'
        #local_destination_path = '/home/jupyter/output'

        if not os.path.exists(local_destination_path):
            os.makedirs(local_destination_path)

        bucket = self.client.get_bucket(bucket_name) 
        blobs = bucket.list_blobs(prefix=bucket_path)

        for blob in blobs:
            destination_path_from_bucket = blob.name[:blob.name.rfind('/')]
            
            if re.findall(r"\.\w+$", bucket_path):
                final_path = os.path.join(local_destination_path, blob.name.replace(destination_path_from_bucket, '').replace('/', ''))
                
            else:
                if not os.path.exists(os.path.join(local_destination_path, destination_path_from_bucket.replace(bucket_path, ''))):
                    os.makedirs(os.path.join(local_destination_path, destination_path_from_bucket.replace(bucket_path, '')))
                    
                final_path = os.path.join(local_destination_path, destination_path_from_bucket.replace(bucket_path, ''), blob.name.replace(destination_path_from_bucket, '').replace('/', ''))
    
    
            blob.download_to_filename(final_path)
            print('File downloaded:', final_path)


    def upload_files(self, bucket_name, destination_bucket_name, local_source_path): 
        #bucket_name = 'test4-134123r1r124r14'
        #destination_bucket_name = 'test3/'
        #local_source_path = '/home/jupyter/output/predict/001/saved_model.pb'

        bucket = self.client.bucket(bucket_name)        

        if re.findall(r"\.\w+$", local_source_path):
            final_destination_path = os.path.join(destination_bucket_name, local_source_path[local_source_path.rfind('/')+1:])
            blob = bucket.blob(final_destination_path)
            blob.upload_from_filename(local_source_path)
            print(f"File {local_source_path} uploaded to Storage Bucket with name {destination_bucket_name} successfully.")
            
        else:
            for root, dirs, files in os.walk(local_source_path):
                for file in files:            
                    final_destination_path = os.path.join(destination_bucket_name, os.path.join(root, file).replace(local_source_path, ''))
                    final_source_path = os.path.join(root, file) 
                    blob = bucket.blob(final_destination_path)
                    blob.upload_from_filename(final_source_path)
                    print(f"File {final_source_path} uploaded to Storage Bucket with name {final_destination_path} successfully.")
        


    def delete_files(self, bucket_name, blob_name):
        #bucket_name = 'model-training-aa8eedc6-d191-4f82-8085-94ef9031cf29'
        #blob_name = 'test/lower_bound/001/saved_model.pb'
        #destination_bucket = [
        #    'test/lower_bound/001/assets/neighbourhood_group_vocab',
        #    'test/lower_bound/001/assets/neighbourhood_vocab',
        #    'test/lower_bound/001/assets/room_type_vocab']

        bucket = self.client.get_bucket(bucket_name) 
        
        if isinstance(blob_name, list):
            for blob_element in blob_name:
                blob = bucket.blob(blob_element)
                blob.delete()
                print("Blob {} deleted.".format(blob_element))
            
        else:
            blob = bucket.blob(blob_name)
            blob.delete()
            print("Blob {} deleted.".format(blob_name))


    def list_files(self, bucket_name, bucket_path):
        #bucket_name = 'model-training-aa8eedc6-d191-4f82-8085-94ef9031cf29'
        #bucket_path = 'tutorials/model-8354960161051246592/tf-saved-model/2021-08-01T16:14:13.897231Z/'

        if not os.path.exists(local_destination_path):
            os.makedirs(local_destination_path)

        bucket = self.client.get_bucket(bucket_name) 
        blobs = bucket.list_blobs(prefix=bucket_path)

        for blob in blobs:
            print(blob.name)

        return [blob for blob in blobs]
            

    def create_bucket(self, bucket_name, bucket_location="us-central1", bucket_class="STANDARD", bucket_project=None):
        try:
            #bucket_name = 'test4-134123r1r124r14'
            #bucket_location = "us-central1" #us-central1 nam4
            #bucket_class = "STANDARD" #STANDARD NEARLINE COLDLINE
            #bucket_project = 'wmt-c4a28f609a0b37a1ac0cc4f614'

            bucket = storage.Bucket(self.client, name=bucket_name)
            bucket.storage_class = bucket_class
            bucket = self.client.create_bucket(bucket, project=bucket_project, location=bucket_location) 
            print("Bucket {} created".format(bucket.name))
        except:
            pass

    def delete_bucket(self, bucket_name): 
        output_sys=os.system(f'gsutil rm -r gs://{bucket_name}')
        print("Bucket {} deleted".format(bucket.name))

        return output_sys


    def close(self):
        self.client.close()


if __name__ == "__main__":
    pass

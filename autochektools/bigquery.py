import json
import logging
import time
from io import BytesIO, StringIO

from google.cloud import bigquery, storage
from google.oauth2 import service_account



class BigQuery:
    def __init__(self, service_account_path, job_config):
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ]

        credentials = service_account.Credentials.from_service_account_file(
            service_account_path, scopes=scopes
        )
        # self.client = bigquery.Client.from_service_account_json(credentials = credentials)
        self.client = bigquery.Client(
            credentials=credentials, project="composite-store-284103"
        )
        self.storageClient = storage.Client.from_service_account_json(
            service_account_path
        )

        self.LOAD_JOB_CONFIG = job_config

        self.logger = logging.getLogger(__name__)

    def upload_to_gcs(
        self,
        bucket_name,
        source_file_path,
        destination_blob_name,
        folder_in_bucket=None,
    ):
        """
        Upload a file to Google Cloud Storage.

        :param bucket_name: Name of the Google Cloud Storage bucket.
        :param source_file_path: Path to the file to be uploaded.
        :param destination_blob_name: Name for the destination blob in the bucket.
        :param credentials_path: Path to the JSON key file for authentication (optional).
        """

        bucket = self.storageClient.bucket(bucket_name)

        if folder_in_bucket:
            destination_blob_name = f"{folder_in_bucket}/{destination_blob_name}"

        blob = bucket.blob(destination_blob_name)

        blob.upload_from_filename(source_file_path)

        public_url = blob.public_url
        public_url = public_url.replace(
            "storage.googleapis.com", "storage.cloud.google.com"
        )

        return public_url

    def create_table(self, dataset_id, table_name, schema):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_name)

        table = bigquery.Table(table_ref=table_ref, schema=schema)

        self.client.create_table(table)

    def check_table(self, dataset_id, table_name):
        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_name)

        try:
            self.client.get_table(table_ref)
            return True
        except Exception:
            return False

    def writeto_bigquery(self, dataset_id, results, table_id):
        table_id = "{}.{}".format(dataset_id, table_id)
        table = bigquery.Table(table_id, schema=self.create_bigqueryschema(results[0]))
        try:
            table = self.client.create_table(table)
            self.logger.info(
                "Created table {}.{}.{}".format(
                    table.project, table.dataset_id, table.table_id
                )
            )
        # except google.api_core.exceptions.Conflict:
        except Exception as e:
            self.logger.error(f"This exception occured whileing creating table: {e}")
            table = self.client.get_table(table_id)
            self.logger.warn(
                "Table {}.{}.{} exists".format(
                    table.project, table.dataset_id, table.table_id
                )
            )
        # check that table rows match the results rows
        self.insert_json(table_id, results)

    def insert_json(self, table_id, item):
        if len(item) != 0:
            try:
                response = self.client.insert_rows_json(table_id, item)
                if response == []:
                    self.logger.info(f"{len(item)} New rows added.")
                else:
                    self.logger.error(
                        f"Encounted Error while inserting rows: {response}"
                    )
            except Exception as e:
                self.logger.error(f"Error {e} occured while inserting data")
            else:
                self.logger.error("No row added.")

    def create_bigqueryschema(self, item):
        schema = set()
        for key in item.keys():
            if (
                isinstance(item[key], str)
                or isinstance(item[key], dict)
                or isinstance(item[key], list)
                or isinstance(item[key], tuple)
            ):
                query_schema = bigquery.SchemaField(key, "STRING")
            elif isinstance(item[key], int):
                query_schema = bigquery.SchemaField(key, "INTEGER")
            schema.add(query_schema)
        return list(schema)

    def insert_rows(self, table_id, rows_to_insert):
        ndjson = self.newline_json(rows_to_insert)
        errors = self.client.insert_rows_json(table_id, ndjson)  # Make an API request.
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    def insert_rows_stringio(self, table_id, rows_to_insert):
        result = "\n".join(json.dumps(row) for row in [rows_to_insert])

        data_io = StringIO(result)
        self.client.load_table_from_file(
            data_io, table_id, job_config=self.LOAD_JOB_CONFIG
        )

        # try:
        #     self.client.load_table_from_file(data_io,table_id,job_config=LOAD_JOB_CONFIG)
        # except Exception as err:
        #     raise
        # errors = self.client.insert_rows_json(table_id, rows_to_insert)  # Make an API request.
        # if errors == []:
        #     print("New rows have been added.")
        # else:
        #     print("Encountered errors while inserting rows: {}".format(errors))

    def insert_kissflow_rows_json(self, table_id, kissflow_id, message):
        rows_to_insert = [{"kissflow_id": kissflow_id, "message": message}]
        errors = self.client.insert_rows_json(table_id, rows_to_insert)
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    def load_data_bigquery(self, dataset_id, table_name, schema, rows_to_insert):
        ndjson = self.newline_json(rows_to_insert)
        ndjson_bytes = ndjson.encode("utf-8")
        ndjson_io = BytesIO(ndjson_bytes)
        if not self.check_table(dataset_id=dataset_id, table_name=table_name):
            self.create_table(
                dataset_id=dataset_id, table_name=table_name, schema=schema
            )

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        # with open('fname_newline.json', 'rb') as f:

        job = self.client.load_table_from_file(
            ndjson_io, f"{dataset_id}.{table_name}", job_config=job_config
        )
        while job.state != "DONE":
            self.logger.info("Reloading and Check if bigquery job completed")
            job.reload()
            time.sleep(2)
        print(job.result())

    def newline_json(self, response_obj):
        return "\n".join(json.dumps(row) for row in response_obj)

    def extract_bigquery(self, statement):
        query_job = self.client.query(statement)  # API request
        results = query_job.result()
        rows = list(results)
        response = [dict(row.items()) for row in rows]
        self.logger.info("Data Extracted Successfully")
        return response

    def fetch_data(self, statement, dataset_id=None, view_id=None):
        self.logger.info("Fetching Data from BigQuery")
        job_config = bigquery.QueryJobConfig()
        job_config.use_query_cache = False
        job_config.use_legacy_sql = False

        if dataset_id and view_id:
            self.client.dataset(dataset_id=dataset_id).table(table_id=view_id)

        query_job = self.client.query(statement, job_config=job_config)

        # Wait for the query to complete
        query_job.result()

        # Fetch the query results into a Pandas DataFrame
        return query_job.to_dataframe()

    def fetch_data_in_chunks(self, query, chunk_size, use_cache=False):
        self.logger.info("Fetching Data from BigQuery")
        job_config = bigquery.QueryJobConfig()
        job_config.use_query_cache = use_cache

        offset = 0
        more_data = True

        while more_data:
            try:
                query_with_limit_offset = f"{query} LIMIT @chunk_size OFFSET @offset"
                job_config.query_parameters = [
                    bigquery.ScalarQueryParameter("chunk_size", "INT64", chunk_size),
                    bigquery.ScalarQueryParameter("offset", "INT64", offset),
                ]

                job = self.client.query(query_with_limit_offset, job_config=job_config)
                df = job.to_dataframe()
            except Exception as e:
                self.logger.error("An error occurred while fetching data: %s", e)
                raise

            if df.empty:
                more_data = False
            else:
                yield df
                offset += chunk_size

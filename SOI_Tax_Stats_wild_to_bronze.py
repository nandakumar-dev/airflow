# Standard Libraries
import os

# Installed Libraries
from airflow.decorators import dag, task
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

#blob_Account
from azure.storage.blob import BlobClient, BlobServiceClient, ContainerClient
from data_common_library.credentials import BlobAccount
from data_common_library.azure import helpers
from data_common_library.azure import client

# Custom Airflow Operators
from databricks_helpers import databricks_submit_run_operator
from raw_to_bronze_operators import RawToBronzeOperatorManual
from wild_to_raw_operators import WildToRawOperatorUrl
from raw_to_bronze_operators import RawToBronzeOperatorCsv

default_args = {
"owner": "Nandakumar A",
}

@dag(
    default_args=default_args,
    schedule=None,
    start_date=pendulum.from_format("2024-04-01", "YYYY-MM-DD").in_tz("UTC"),
    catchup=False,
    owner_links={
        "Nandakumar A": "mailto:v-nandakumara@usafacts.org",
    },
)


def SOI_Tax_wild_to_bronze():
    
    scraped_urls_to_raw= databricks_submit_run_operator(
        task_id='SOI_Tax_scrap_urls_to_raw',
        notebook_path=f"{os.getenv('BASE_DATABRICKS_PATH')}/Url_scraping/wild_url_scrap_to_ingeastion_meta",
        
        notebook_parameters = {
            # scrap_url_path should be after the ingestion-meta container under source_urls directory
            'url' : '',
            'scrap_url_path' : 'SOI/SOI_Tax_Stats_Urls.csv',
            'patterns' : '.xls,.xlsx'
            #Note: Output will be stored in the base url path in ingestion-meta container under scraped_urls directory.
            
        }
    )

    ingest_files_wild_to_raw= databricks_submit_run_operator(
        task_id='SOI_Tax_ingest_files_wild_to_raw',
        notebook_path=f"{os.getenv('BASE_DATABRICKS_PATH')}/Url_scraping/source_to_raw_and_unzip_files_patterns_to_meta_ingestion",
        

        notebook_parameters = {
            # input_files_blob_path should be after the ingestion-meta container under scraped_urls directory
            'input_files_blob_path' : 'www.irs.gov/',
            'patterns':''
            }
    )

    SOI_Tax_raw_wild_to_raw= databricks_submit_run_operator(
        task_id='SOI_Tax_files_wild_to_raw',
        notebook_path=f"{os.getenv('BASE_DATABRICKS_PATH')}/SOI Tax Stats/SOI_Tax_Stats_wild_to_raw",
        
        notebook_parameters = {
            # blob_relative_path should be after the to-wild container
            #record_layout_path should be after the to-wild container
            'blob_relative_path' : 'www.irs.gov/pub/irs-soi/',
            'ingestion_meta_path' : ''
            }
    )

    SOI_Tax_raw_to_bronze=databricks_submit_run_operator(
        task_id='SOI_Tax_files_raw_to_bronze',
        notebook_path=f"{os.getenv('BASE_DATABRICKS_PATH')}/SOI Tax Stats/SOI_Tax_Stats_raw_to_bronze",
        
        notebook_parameters = {
            # blob_relative_path should be after the to-bronze container
            'pipeline_id':'54c7c819-7f45-4c39-a8af-8971c642e384',
            'job_id':'394e47d0-df5e-4d83-91f6-c71d4a09c27b',
            'blob_relative_path':'www.irs.gov/pub/irs-soi/'
            }
    )

    scraped_urls_to_raw >> ingest_files_wild_to_raw >> SOI_Tax_raw_wild_to_raw >> SOI_Tax_raw_to_bronze

dag_obj = SOI_Tax_wild_to_bronze()
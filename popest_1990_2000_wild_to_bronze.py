# Standard Libraries
import os
import re

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
    start_date=pendulum.from_format("2024-02-20", "YYYY-MM-DD").in_tz("UTC"),
    catchup=False,
    owner_links={
        "Nandakumar A": "mailto:v-nandakumara@usafacts.org",
    },
)


def popest_1980_2000_wild_to_bronze():

    pipeline_id='f0bc43d7-a30f-4fdb-b094-55befa667d24'
    
    scraping_urls_to_wild= databricks_submit_run_operator(
        task_id='popest_1980_2000_scraping_urls_to_wild',
        notebook_path=f"/Repos/v-nandakumara@usafacts.org/usafacts-data-platform-notebooks/Url_Scraping/Scraping_meta_info_wild_To_to_wild",#f"{os.getenv('BASE_DATABRICKS_PATH')}/usafactssilver_to_tobronze",
        notebook_parameters = {
            # path should be after the ingestion-meta container under source_urls directory
            'input_Url_file_blob_path' : 'census/popest/popest_datasets.csv'
            #Note: Output will be stored in the same path in ingestion-meta container under scraped_urls directory.
            
        }
    )

    ingesting_files_source_to_wild= databricks_submit_run_operator(
        task_id='popest_1980_2000_ingesting_files_source_to_wild',
        notebook_path=f"/Repos/v-nandakumara@usafacts.org/usafacts-data-platform-notebooks/Url_Scraping/Source_to_Raw_And_Unzipping_Zipp_Files_patterns_wild_To_to_wild",

        notebook_parameters = {
            # input_files_blob_path should be after the ingestion-meta container under scraped_urls directory
            'input_files_blob_path' : 'census/popest/popest_datasets.csv',
            'patterns':'\d+0(?:/[^/]+)*/[^/]+\.zip$'
            }
    )

    mapping_files_to_wild_To_to_bronze= databricks_submit_run_operator(
        task_id='popest_1980_2000_mapping_files_to_wild_To_to_bronze',
        notebook_path=f"/Repos/v-nandakumara@usafacts.org/usafacts-data-platform-notebooks/Notebooks/DL-196 popest txt conversion/Census_Popest_1980_2000_txt_to_bronze",
        
        notebook_parameters = {
            # blob_relative_path should be after the to-wild container
            #record_layout_path should be after the to-wild container
            'blob_relative_path' :'www2.census.gov/programs-surveys/popest/datasets/',
            'patterns': '.*/[Ee]\d{4}[A-Za-z]{3}\.(?i:txt)$',
            'record_layout_path': 'www2.census.gov/programs-surveys/popest/datasets/POPEST text file layout 1980-2000.xlsx'
            }
    )

    def get_file_paths(blob_relative_path, patterns):
        if not isinstance(blob_relative_path, str):
            blob_relative_path = blob_relative_path.value
        blob_account = BlobAccount.MANUAL_ACCOUNT
        blob_container = BlobAccount.MANUAL_RAW_TO_BRONZE_CONTAINER
        container_client = client.create_container_client(blob_account, blob_container)
        
        blobs = [blob.name for blob in container_client.list_blobs() if blob_relative_path in blob.name]
        
        if patterns:
            regex_patterns = [re.compile(pattern) for pattern in patterns.split(',')]
            filtered_urls = [url for url in blobs for pattern in regex_patterns if pattern.search(url)]
            return filtered_urls
        else:
            return blobs

    def create_task_for_file(file_name, pipeline_id):
        user = 'Nandakumar A'
        name = file_name.split('/')[-1].split('.')[0]

        acs_public_data_to_bronze_TO_bronze = RawToBronzeOperatorManual(
            task_id="popest_1980_2000_to_bronze_TO_bronze_" + str(name),
            pipeline_id=pipeline_id,
            created_by=user,
            description=f"to_bronze TO bronze for popest 1980-2000_{name}",
            filename=file_name,
            retries=3
        )
        return acs_public_data_to_bronze_TO_bronze
    
    execute_function = PythonOperator(
        task_id='scraping_blob_path',
        python_callable=get_file_paths,
        op_kwargs={'blob_relative_path': 'www2.census.gov/programs-surveys/popest/datasets/',
                   'patterns' :  '.*/[Ee]\d{4}[A-Za-z]{3}\.(?i:csv)$'}   
    )

    return_values = execute_function.execute(context={})

    scraping_urls_to_wild >> ingesting_files_source_to_wild >> mapping_files_to_wild_To_to_bronze >> execute_function

    for value in return_values:
        task = create_task_for_file(value,pipeline_id)
        execute_function >> task

dag_obj = popest_1980_2000_wild_to_bronze()
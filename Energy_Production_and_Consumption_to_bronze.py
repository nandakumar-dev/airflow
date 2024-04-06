# Standard Libraries
import os

# Installed Libraries
from airflow.decorators import dag, task
import pendulum

# Custom Airflow Operators
from wild_to_raw_operators import WildToRawOperatorUrl
from raw_to_bronze_operators import RawToBronzeOperatorCsv

files=['csv.php?tbl=T01.02','csv.php?tbl=T01.03']
pipeline_ids=['1a9ad14c-f1bf-4d51-bee3-9b82fc14415e','e9781352-0da6-4a96-9075-491d11ddbabc']

default_args = {
"owner": "Nandakumar A",
}

def create_task_for_file(file_name, pipeline_id):

    user = 'Nandakumar A'
    url = f'https://www.eia.gov/totalenergy/data/browser/{file_name}'
    file_name=file_name.split('=')[-1].replace('.','_')

    def EIA_file_wild_To_to_wild():
        return WildToRawOperatorUrl(
            task_id=f"Energy_Production_and_Consumption_wild_To_to-wild-{file_name}",
            pipeline_id=pipeline_id,
            file_url=url,
            created_by=user,
            description=f"EIA_Energy_Production_and_Consumption to wild - {file_name}",
            retries=3
        )
    
    def EIA_file_to_wild_To_to_bronze():
        return RawToBronzeOperatorCsv(
            task_id=f"Energy_Production_and_Consumption_to-wild_To_to-bronze-{file_name}",
            pipeline_id=pipeline_id,
            created_by=user,
            description=f"EIA_Energy_Production_and_Consumption to bronze - {file_name}",
            delimiter=',',
            agency='EIA_Energy_Production_and_Consumption',
            table_name=f"Energy_Production_and_Consumption_{file_name}",
            retries=3
        )

    return EIA_file_wild_To_to_wild(), EIA_file_to_wild_To_to_bronze()

@dag(
    default_args=default_args,
    schedule=None,
    start_date=pendulum.from_format("2024-03-25", "YYYY-MM-DD").in_tz("UTC"),
    catchup=False,
    owner_links={
        "Nandakumar A": "mailto:v-nandakumara@usafacts.org",
    },
)
def Energy_Production_and_Consumption_wild_to_bronze():

    for file_name ,pipeline_id in zip(files,pipeline_ids):
        wild_to_raw_task, raw_to_bronze_task = create_task_for_file(file_name,pipeline_id)
        
        wild_to_raw_task >> raw_to_bronze_task

dag_obj = Energy_Production_and_Consumption_wild_to_bronze()
# Standard Libraries
import os

# Installed Libraries
from airflow.decorators import dag, task
import pendulum
 
# Custom Airflow Operators
from databricks_helpers import databricks_submit_run_operator
from raw_to_bronze_operators import RawToBronzeOperatorManualDelta

default_args = {
"owner": "Nandakumar A",
}

@dag(
    default_args=default_args,
    schedule=None,
    start_date=pendulum.from_format("2024-03-22", "YYYY-MM-DD").in_tz("UTC"),
    catchup=False,
    owner_links={
        "Nandakumar A": "mailto:v-nandakumara@usafacts.org",
    },
)

def BEA_Regional():

    pipeline_id='f32e9273-87d6-4ff4-aee3-84c1aad6c024'
    user = 'Nandakumar A'
    
    BEA_Regional_to_wild= databricks_submit_run_operator(
        task_id='BEA_Regional_source_to_wild',
        notebook_path=f"{os.getenv('BASE_DATABRICKS_PATH')}/BEA_Regional_State_Metro_County_Data/BEA_Regional_source_To_to_wild",
        notebook_parameters = {
            'run_id' : f'{pipeline_id}'    
        }
        )

    BEA_Regional_to_wild_To_to_bronze= databricks_submit_run_operator(
        task_id='BEA_Regional_to_wild_To_to_bronze',
        notebook_path=f"{os.getenv('BASE_DATABRICKS_PATH')}/BEA_Regional_State_Metro_County_Data/BEA_Regional_to_wild_To_to_bronze",
        )

    BEA_Regional_combined_to_bronze= databricks_submit_run_operator(
        task_id='BEA_Regional_combined_to_bronze',
        notebook_path=f"{os.getenv('BASE_DATABRICKS_PATH')}/BEA_Regional_State_Metro_County_Data/BEA_Regional_combined_to_bronze",  
        )

    BEA_Regional_combined_to_bronze_To_bronze=RawToBronzeOperatorManualDelta(
            pipeline_id= pipeline_id, 
            created_by= user, 
            description= 'BEA_Regional_combined_to_bronze_To_bronze', 
            filename= 'BEA/BEA_Regional/BEA_Regional_combined', #to_bronze delta file path
            task_id='BEA_Regional_combined_to_bronze_To_bronze',
            agency='BEA',
            table_name=f"BEA_Regional_State_Metro_County",
            retries=3
            )

    BEA_Regional_to_wild >> BEA_Regional_to_wild_To_to_bronze >> BEA_Regional_combined_to_bronze >> BEA_Regional_combined_to_bronze_To_bronze

dag_obj = BEA_Regional()
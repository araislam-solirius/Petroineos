import sys
import os
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pendulum import datetime
from datetime import datetime
# Setting the PYTHONPATH to include the main project directory
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from include.main import check_latest_data, get_Df, save_cleaned_file

# Define the DAG
@dag(
    start_date=datetime(2024, 10, 10),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "MohammedIslam", "retries": 1},
    tags=["ara_test_dag"],
)
# Define the Dag function
def petroineos_dag():

    @task
    def check_latest_data_task():
        """
        This task checks the latest data availability.
        """
        return check_latest_data()  # Return True or False based on data availability

    @task
    def get_df_task(latest_data_available):
        """
        This task retrieves the data as a DataFrame if new data is available.
        """
        if latest_data_available:  # Check if new data is available
            return get_Df(latest_data_available)  # Pass the parameter to get_Df
        else:
            return None  # Return None if no new data is available

    @task
    def save_cleaned_file_task(df, file_name) -> None:
        """
        This task saves the cleaned DataFrame to a file.
        """
        if df is not None:  # Check if df is not None before saving
            save_cleaned_file(df, file_name)  # Call the function from your module

    # Set task dependencies 
    latest_data = check_latest_data_task()
    df = get_df_task(latest_data)  # Pass the result of check_latest_data_task

    # Define the file name for saving the cleaned DataFrame
    file_name = f'Clean_Crude_Oil_Supply_Use_{datetime.today().strftime('%Y-%m')}'
    save_cleaned_file_task(df, file_name)  # Pass the DataFrame and the file name

# Create an instance of the DAG
my_dag_instance = petroineos_dag()

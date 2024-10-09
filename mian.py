from bs4 import BeautifulSoup
import requests
import json
from datetime import datetime
import pandas as pd
from tenacity import retry, stop_after_attempt,wait_exponential
import logging
import os
import numpy as np

logging.basicConfig(filename='main.log',level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger(__name__)
# get the cached last modified and file_name

url = "https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends"
cache_file = 'cache.json'

def load_cache():
    """
    This function retrives the stored values for the last modifies data and latest file name from the url
    """
    cache_file = 'cache.json'
    try:
        with open(cache_file,'r') as file:
            cache = json.load(file)
            cached_last_modified = datetime.fromisoformat(cache['cached_last_modified'])
            cached_file_name = cache['cached_file_name']
            logger.info(f"Retrived the last cahced Modied date: {cached_last_modified} and last cached File Name: {cached_file_name}")
    except (FileNotFoundError, KeyError, ValueError) as e:
        logger.error(f"Cache not found or malformed: {e}. Using default values")
        # If the cache file does not exist or has errors, return default values
        cached_last_modified = datetime.fromisoformat('2000-01-01T09:30:13+01:00')
        cached_file_name = 'Supply and use of crude oil, natural gas liquids and feedstocks (ET 3.1 - quarterly)'
    return cached_last_modified, cached_file_name



def save_cache(new_cached_last_modified,new_cached_file_name):
    """
    This function will update the stored values of the last modified data and last file name from the url
    """
    cache_file = 'cache.json'
    cache_data = {
        'cached_last_modified' : new_cached_last_modified,
        'cached_file_name' : new_cached_file_name
    }
    with open(cache_file,'w') as file:
        json.dump(cache_data,file)



@retry(stop=stop_after_attempt(5),wait=wait_exponential())
def get_request(url):
    """
    attemps to get a response message from the url, it will retry up tp 5 times with an exponential back off after each attempt.
    """
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f'Failed to fetch data: {response.status_code}')
    return response

def check_latest_data(url = 'https://www.gov.uk/government/statistics/oil-and-oil-products-section-3-energy-trends'):
    """
    This function check the goverment website for a new version of Supply and use of crude oil, natural gas liquids, and feedstocks dataset
    """

    # Encoding format we are looking for
    encoding_format = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    # get the cached last modified and file name
    cached_last_modified, cached_file_name = load_cache()
    

    # Make a request to fetch the page content
    try:
        response = get_request(url)
        soup = BeautifulSoup(response.content, 'html.parser')
    except Exception as e:
        logger.error(f"There was an error gathering the html data {e}")

    # Find all the relevant script tags
    script_tags = soup.find_all('script', type='application/ld+json')


    # Loop through the script tags
    for script in script_tags:
        try:
            # Parse the JSON data within the script tag
            json_data = json.loads(script.string)

            if json_data['@type']== 'Dataset':

                # Extract the modified date and file name, if available
                try:
                    current_date_modified = datetime.fromisoformat(json_data['dateModified'])
                    logger.info(f"The Latest URL Modiefied date retrived is {current_date_modified}")
                except KeyError:
                    logger.error("No 'dateModified' found in this script block")
                    continue


                # Check if the file has a later modified date and a different name
                if current_date_modified > cached_last_modified:
                    # Check if the 'distribution' contains the correct encodingFormat
                    if 'distribution' in json_data:
                        for item in json_data['distribution']:
                            if 'encodingFormat' in item and item['encodingFormat'] == encoding_format and item['name']=='Supply and use of crude oil, natural gas liquids and feedstocks (ET 3.1 - quarterly)':
                                # if the the page has been modified since last check point and the file name has changed pull new data
                                if cached_file_name != item['contentUrl']:
                                    # Add the download link to the list
                                    new_file_name = item['contentUrl']

                                    save_new_file(new_file_name,current_date_modified)
                                    # Update the cache with the new file name and modified date

                                    logger.info(f"Caching New file found: {item['contentUrl']}, caching last modified: {current_date_modified}")
                                    save_cache(str(current_date_modified),item['contentUrl'])
                                else:
                                    logger.info(f"No New file name found: file {item['contentUrl']} is the same as {cached_file_name}, last modified: {current_date_modified}")
                else:
                    logger.info('There is no new file for the Supply and use of crude oil, natural gas liquids, and feedstocks. ')

        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {e}")
        except Exception as e:
            logger.error(f"Encountered an error: {e}")



def save_new_file(url,current_date_modified):
    """
    This Function will save the URL found in to project folder as RAW excel files
    """

    month_year = current_date_modified.strftime('%B %Y').replace(' ','_')

    save_path = os.path.join(os.getcwd(), 'Raw_Files', "".join(['Crude_Oil_Supply_Use_ET3.1_',month_year,'.xlsx']))

    # Create the directory if it doesn't exist
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    # Download the file
    response = get_request(url)

    # Check if the request was successful
    if response:
        with open(save_path, 'wb') as file:
            file.write(response.content)  # Write the binary content to a file
        print(f"File saved at: {save_path}")
    else:
        print(f"Failed to download file")



check_latest_data()

import glob
# process raw files
def get_latest_raw_file():
    # Get the latest file from the raw folder based on the created date
    folder_path = 'Raw_Files/*.xlsx'
    files = glob.glob(folder_path)
    return max(files,key=os.path.getctime)


# Logic to skip rows if they are not relevant
def skip_rows(rows):
    """
    This Function provides logic to find the number of rows to skip to get a clean DataFrame
    """
    try:
        for i, row in rows.iterrows():
            if str(row.iloc[0])=='Column1':
                return i+1
    except Exception as e:
        logger.error(f"failed to find the number of rows to skip: {e}")

def get_Df():
    """
    This funciton will apply initial logic such as selecting the correct sheet for the data and skipping bad rows

    Return: DataFrame for further processing
    """
    initial_rows = pd.read_excel(get_latest_raw_file(),sheet_name='Quarter')
    rows_to_skip = skip_rows(initial_rows)

    df = pd.read_excel(get_latest_raw_file(),sheet_name='Quarter',skiprows=rows_to_skip)

    # Remove anything in square brackets (e.g., "[note]") from the DataFrame's values
    df.replace(r"\[.*?\]",'',regex=True,inplace=True)

    # clean Column and names and rename Column1 to Key
    df.columns = [col.replace('\n', " ").strip() for col in df.columns]
    df.rename(columns={'Column1':"Key"},inplace=True)
    df.set_index("Key",inplace=True)
    return df


df = get_Df()


# 1.b) cleaned first column and make it the index

def save_cleaned_file(df,file_name):
    """
    This function test if the Dataframe index and row count matchs the expected configuration, It also check that there are no missing values

    Return: Saves the Dataframe as a csv file if it passes all the tests
    """

    clean_folder_path = os.path.join(os.getcwd(),'Clean_Files')
    os.makedirs(clean_folder_path)

    index = df.index.values

    try:
        with open('cache.json','r') as file:
            cache = json.load(file)
            cached_index = cache['index']
            cached_row_count = cache["row_count"]
            assert np.array_equal(index, cached_index ) # Check the index matchs
            assert(df.shape[0]==cached_row_count) # Check the row count matchs
            assert df.isna().sum().sum()==0 # Check are there any missing values in the DataFrame
    except (AssertionError, FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error occurred: {e}")
        return  # Exit early, do not save the file
    save_path = os.path.join(clean_folder_path,f"{file_name}.csv")
    df.to_csv(save_path,index=False)
    print(f"Cleaned DataFrame saved to {save_path}")

save_cleaned_file(df,'Clean_Crude_Oil_Supply_Use_ET3.1_September_2024')



    

    







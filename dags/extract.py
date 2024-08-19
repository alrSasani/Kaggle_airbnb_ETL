from airflow.decorators import task
from kaggle.api.kaggle_api_extended import KaggleApi
import os


@task(task_id = 'EXTRACT')
def extract(dataset_name,  downloads_path):
    """ Download airbnb dataset from Kaggle using the Kaggle API and store 
        data into 'downloads_path' directory.

    Args:
        dataset_name (str): Kaggle API key
        downloads_path (str): path to downloads directory
    
    """
    print('Extractin airbnb data using Kaggle api from Kaggle database...')
    api = KaggleApi()
    api.authenticate()

    # Download files
    if not os.path.exists(downloads_path):
        os.makedirs(downloads_path)

    # Cheking if the data is already downloaded.
    if (not (os.path.isfile(os.path.join(downloads_path,'listings.csv'))) or 
        not (os.path.isfile(os.path.join(downloads_path,'reviews.csv'))) or 
        not (os.path.isfile(os.path.join(downloads_path,'calendar.csv'))) ):
        api.dataset_download_files(dataset_name,
                            unzip=True, path=downloads_path,force=True)
    
    print(downloads_path)
    print(f'Data downloaded to {downloads_path} path!')

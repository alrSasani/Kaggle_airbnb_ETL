import pandas as pd
from airflow.decorators import task
import os

@task(task_id = 'TRANSFORMATION')
def transform(csv_files,extraction_path,transformed_path):
    """ 
    Transformations:
        calendar_csv:
            - 1. remove adjusted_price from calender.csv all Null
            - 2. change available col to bool type (f,t -> False,True)
            - 3. Change price col to float with $ sign in col name
            - 4. other than adjusted_price, data has no null values but two null values 
            on minimum_night and maximum_nights we can remove them?
        listing_csv:
            the data frame is good and it will be returned 
        review_csv:
            1. remove the comment columns with null values.
        defining the data types for our columns

    Args:
        csv_files: names of CSV files extracted.
        extraction_path: Path, where the data extracted
        transformed_path: Path,  path to save transformed csv files.
    """

    print('Transforming the data...')
    # Importing data in  a dictinary to work with in pandas dataframe format:
    df_dict = {} 
    for csv_file in csv_files:
        key = csv_file.split('.')[0]
        index_clo = 'id' if csv_file=='listings.csv' else 'listing_id'
        df_dict[key]  = pd.read_csv(f'{extraction_path}/{csv_file}',index_col=index_clo)
        print(csv_file,'File extarcted...')  

    if not os.path.exists(transformed_path):
        os.makedirs(transformed_path)

    # transforming columns name to lower case
    for df_key in df_dict.keys():
        df_dict[df_key].columns = df_dict[df_key].columns.map(lambda x: x.lower())

    # Remove columns/rows with missing values
    # removing columns with no comments from reviews data:
    df_dict['reviews'] = df_dict['reviews'][df_dict['reviews']['comments'].notnull()]
    # adjusted_price columns in calendar is null 
    df_dict['calendar'].drop(columns=['adjusted_price'],inplace=True,axis=1)
    # transforming  available columns to boolean:
    df_dict['calendar']['available'] = df_dict['calendar']['available'].map({'t':True,'f':False}).astype(bool)
    # transfomrning price columns from text with $ sign to float values
    df_dict['calendar']['price'] = df_dict['calendar']['price'].str.translate({ord(','): None,ord('$'): None}).astype(float)


    # redefining columns data types:
    df_dict['calendar'] = df_dict['calendar'].astype({'date':object,
                                                        'available':bool,
                                                        'minimum_nights':float,
                                                        'maximum_nights':float})
    

    df_dict['reviews']= df_dict['reviews'].astype({'id':int,'date':object,
                                                    'reviewer_id':int,
                                                    'reviewer_name':str,
                                                    'comments':str}) 
 
    df_dict['listings']=df_dict['listings'].astype({'name':object, 'host_id':int,
        'host_name':object,'neighbourhood_group':object, 'neighbourhood':object,
        'latitude':float, 'longitude':float, 'room_type':object, 'price':float,
        'minimum_nights':int,'number_of_reviews':int, 'last_review':object,
        'reviews_per_month':float,'calculated_host_listings_count':int, 'availability_365':int,
        'number_of_reviews_ltm':int, 'license':object})


    # saving csv file to transformed_path
    df_dict['listings'].to_csv(os.path.join(transformed_path,'listings.csv'))
    df_dict['reviews'].to_csv(os.path.join(transformed_path,'reviews.csv'))
    df_dict['calendar'].to_csv(os.path.join(transformed_path,'calendar.csv'))
    print('Data transformed and saved to csv file in {transformed_path}!')


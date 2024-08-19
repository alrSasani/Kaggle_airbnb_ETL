import psycopg2
import pandas as pd
from airflow.decorators import task

@task
def load(csv_files,transform_path, db_params):
    """ A function to load transformed data frames  to Postgres database.

    Args:
        csv_files: files transformes and to be loaded to database.
        transform_path: Path where the data saved after transformation.
        db_params (dict): parameters for Postgres connection
    
    """
    print("reading transformed csv files...")

    df_dict = {} 
    for csv_file in csv_files:
        key = csv_file.split('.')[0]
        index_clo = 'listing_id'
        if csv_file=='listings.csv':
            index_clo = 'id'
        df_dict[key]  = pd.read_csv(f'{transform_path}/{csv_file}',index_col=index_clo)
        print(csv_file,'File extarcted...')  

    # Some transformation to make the Null data type compatible with postgresl.
    df_dict['listings']['last_review'] = df_dict['listings']['last_review'].apply(lambda x: None if pd.isna(x) else x)
    df_dict['reviews']['date'] = df_dict['reviews']['date'].apply(lambda x: None if pd.isna(x) else x)
    df_dict['calendar']['date'] = df_dict['calendar']['date'].apply(lambda x: None if pd.isna(x) else x)

    db_name  = db_params['db_name']
    user=db_params['user_name']

    print(f'Connecting to database {db_name} as {user}...')

    # Connection to Postgres database
    connection = psycopg2.connect(database=db_name,
                                  host=db_params['host'],
                                  user=user,
                                  password=db_params['password'],
                                  port=db_params['port'])
    cursor = connection.cursor()

    print('Loading data...')
    # Create and fill listings table

    query_create_table = """DROP TABLE IF EXISTS listings CASCADE;\
    CREATE TABLE listings (\
    id NUMERIC PRIMARY KEY,\
    name VARCHAR,\
    host_id NUMERIC,\
    host_name VARCHAR,\
    neighbourhood_group VARCHAR,\
    neighbourhood VARCHAR,\
    latitude DOUBLE PRECISION,\
    longitude DOUBLE PRECISION,\
    room_type VARCHAR,\
    price NUMERIC,\
    minimum_nights NUMERIC,\
    number_of_reviews NUMERIC,\
    last_review DATE,\
    reviews_per_month NUMERIC,\
    calculated_host_listings_count NUMERIC,\
    availability_365 NUMERIC,\
    number_of_reviews_ltm NUMERIC,\
    license VARCHAR);"""

    cursor.execute(query_create_table)
    connection.commit()

    query_insert = """INSERT INTO listings (id, name,\
    host_id, host_name, neighbourhood_group, neighbourhood, latitude, longitude,\
    room_type, price, minimum_nights, number_of_reviews, last_review, reviews_per_month,\
    calculated_host_listings_count, availability_365, number_of_reviews_ltm, license ) VALUES\
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""  

    for index, row in df_dict['listings'].iterrows():  
        cursor.execute(query_insert,(index,row['name'],row['host_id'],row['host_name'],
                     row['neighbourhood_group'],row['neighbourhood'],row['latitude'],
                     row['longitude'],row['room_type'],row['price'],row['minimum_nights'],
                     row['number_of_reviews'],row['last_review'],row['reviews_per_month'],
                     row['calculated_host_listings_count'],row['availability_365'],
                     row['number_of_reviews_ltm'],row['license']))

    connection.commit()

    print('\n>> listings table loaded!')

# Create and fill reviews table:

    query_create_table = """DROP TABLE IF EXISTS reviews CASCADE;\
        CREATE TABLE IF NOT EXISTS reviews (\
        listing_id NUMERIC,\
        id NUMERIC,\
        date DATE,\
        reviewer_id NUMERIC,\
        reviewer_name VARCHAR,\
        comments TEXT);"""

    cursor.execute(query_create_table)

    query_insert = """INSERT INTO reviews (listing_id , id, date, reviewer_id, reviewer_name,\
                  comments) VALUES (%s, %s, %s, %s, %s, %s);""" 
    cntr = 0
    for index, row in df_dict['reviews'].iterrows():        
        cursor.execute(query_insert, (index,row['id'],row['date'],row['reviewer_id'],
                                      row['reviewer_name'],row['comments']))

    connection.commit()
    print('\n>> reviews table loaded!')

# Create and fill calendar table
    query_create_table = "DROP TABLE IF EXISTS calendar CASCADE;\
      CREATE TABLE IF NOT EXISTS calendar (\
      listing_id INTEGER,\
      date DATE,\
      available BOOLEAN,\
      minimum_nights NUMERIC,\
      maximum_nights NUMERIC);"
    
    cursor.execute(query_create_table)

    query_insert = """
    INSERT INTO calendar (listing_id, date, available, minimum_nights, maximum_nights)
    VALUES (%s, %s, %s, %s, %s);
    """

    # Insert rows from DataFrame
    cnt=0
    for index, row in df_dict['calendar'].iterrows():
        cursor.execute(query_insert, (index, row['date'], row['available'], row['minimum_nights'], row['maximum_nights']))
        cntr+=1
        if cntr > 10000:
            break
    connection.commit()

    print('\n>> Calendar table loaded!')

    # Close connection to database
    cursor.close()
    connection.close()

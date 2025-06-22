import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

logging.basicConfig(
   filename="C:\\Users\\HP\\VendorData\\logs\\ingestion_db.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s-%(message)s",
    filemode="a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    '''this function will ingest the dataframe into database table'''
    df.to_sql(table_name, con = engine, if_exists='replace', index = False, chunksize=1000)
    
folder_path = 'D://Vendor Data Analysis//data//data' 
def load_raw_data():
    '''this function will load the CSVs as dataframe and ingest into db'''
    start = time.time()
    for file in os.listdir(folder_path):
        if file.endswith('.csv'):
            full_path = os.path.join(folder_path, file)
            df = pd.read_csv(full_path)
            logging.info(f'Ingesting {file} in db')
            ingest_db(df, file[:-4], engine)
    end = time.time()
    total_time = (end - start)/60
    logging.info('------------Ingestion Complete-----------')
    logging.info(f'\n Total Time Taken: (Total_time) minutes')
    
if __name__ == '__main__':
    load_raw_data()
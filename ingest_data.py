#!/usr/bin/env python
# coding: utf-8

import glob
import pandas as pd
from time import time
import argparse, os
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url_rides = params.url_rides
    url_zones = params.url_zones
    csv_name = 'output.csv'
    
    # download csv
    os.system(f'wget {url_rides}')
    os.system(f'wget {url_zones}')
    
    # Get data file names
    path1 = './'
    filenames = glob.glob(path1 + "/*.csv.gz")
    dfs = []
    for filename in filenames:
        dfs.append(pd.read_csv(filename))
    pd.concat(dfs, ignore_index=True).to_csv('output.csv', index=False)
    
    


    # Create engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    
    # Schema properties
    # print(pd.io.sql.get_schema(df, name='green_taxi_data', con=engine))

    # transform data
    # df_trans = pd.read_parquet(csv_name,  engine='auto').to_csv('output.csv',index=False)

    # Load data
    df_iter = pd.read_csv('output.csv', iterator=True, chunksize=100000)
    df = next(df_iter)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    # create a table
    df.head(0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')

    # load first batch
    df.to_sql(name='green_taxi_data', con=engine, if_exists='append')
    # load the rest data
    while True:
        try:
            t_start = time()
            
            df = next(df_iter)
            
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            
            df.to_sql(name='green_taxi_data', con=engine, if_exists='append')
            
            t_end = time()
            
            print('Inserted another chunk......, took %.3f seconds' % (t_end - t_start))
        except StopIteration as E:
            print('Completed.....')
            break
        
            
    # load zones
    taxi_zone = pd.read_csv('taxi+_zone_lookup.csv')
    taxi_zone.to_sql(name='taxi_zone', con=engine, if_exists='replace')
    
    
if __name__ == '__main__':    
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='username for postgres')
    parser.add_argument('--host', help='username for postgres')
    parser.add_argument('--port', help='username for postgres')
    parser.add_argument('--db', help='username for postgres')
    parser.add_argument('--url_rides', help='username for postgres')
    parser.add_argument('--url_zones', help='username for postgres')

    args = parser.parse_args()

    main(args)

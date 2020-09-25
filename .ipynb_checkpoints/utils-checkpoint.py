import boto3
import csv
import os
from sqlalchemy import create_engine
import pymysql
import re
import numpy as np
import pandas as pd
    
def download_s3_folder(aws_access_key_id,aws_secret_access_key,region_name,bucket_name, s3_folder, local_dir=None):
    print("Authenticating AWS Credentials")
    session = boto3.Session(
                aws_access_key_id = aws_access_key_id,
                aws_secret_access_key = aws_secret_access_key,region_name = region_name)
    
    s3 = session.resource('s3')
    print("Authenticated AWS Credentials")
    
    print("Downloading files from bucket")
    bucket = s3.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix = s3_folder):
        target = obj.key if local_dir is None \
            else os.path.join(local_dir, os.path.basename(obj.key))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if 'json' in obj.key:
            bucket.download_file(obj.key, target)
    print("Files have been downloaded")

def concat_json_files(folder):
    df = pd.DataFrame()
    for f in os.listdir(folder):
        if 'json' in f:
            df = pd.concat([df, pd.read_json(folder + '/' + f)])
    return df.reset_index().drop('index', 1)

def sql_connect(username,password,connection_string,database):
    sqlEngine       = create_engine('mysql+pymysql://'+username+':'+password+'@'+connection_string+'/'+database, pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    return dbConnection

def df_to_table(dbConnection,tableName,dataFrame):
    try:
        frame           = dataFrame.to_sql(tableName, dbConnection, if_exists='fail')
    except ValueError as vx:
        print(vx)
    except Exception as ex:   
        print(ex)
    else:
        print("Table %s created successfully."%tableName)

def preprocess(df):
    pd.set_option('display.float_format', lambda x: '%.2f' % x)
    df.drop_duplicates(subset=['upc', 'store_number', 'department', 'category', 'name' ])
    df['price'] = df.price.astype('str').apply(lambda x: np.nan if len(re.findall(r'^[A-Za-z_.]+$', x)) != 0 else x).astype('float64')
    df['upc'] = df.upc.astype('str').apply(lambda x: np.nan if len(re.findall(r'^[A-Za-z_.]+$', x)) != 0 else 
                                             (np.nan if (x == '')  else x)).astype('float64')
    df['store_number'] = df.store_number.astype('str').apply(lambda x: np.nan if len(re.findall(r'^[A-Za-z_.]+$', x)) != 0 else
                                                               (np.nan if (x == '')  else x)).astype('float64')
    return df.dropna()

def read_from_db(database,tablename,dbConnection):
    df = pd.read_sql('select * from ' + database + '.'+ tablename, dbConnection)
    return df

def get_product(upc,store_number):

    return df.loc[(df["upc"] == float(upc)) & (df["store_number"] == float(store_number))]
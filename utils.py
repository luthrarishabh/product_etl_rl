import boto3
import csv
import os
from sqlalchemy import create_engine
import pymysql
import re
import numpy as np
import pandas as pd
import gzip
import json   
def download_s3_folder(aws_access_key_id,aws_secret_access_key,region_name,bucket_name, s3_folder, local_dir=None):
    """
    This function authenticates the user keys to AWS and can download the folder
    Args:
        aws_access_key_id: the access key id given as a user credential
        aws_secret_access_key: the secret access key id given as a user credential
        region_name: region(mostly ca-central-1)
        bucket_name: name of bucket to download files from
        s3_folder: folder in which files are located
        local_dir: name of the local directory
    """
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
    """
    ***THIS FUNCTION IS DISCARDED***
    Given a folder of json files, this function will concatenate them all into 1 dataframe
    Args: folder name containing multiple json files
    """
    df = pd.DataFrame()
    for f in os.listdir(folder):
        if 'json' in f:
            df = pd.concat([df, pd.read_json(folder + '/' + f)])
    return df.reset_index().drop('index', 1)

def json_concat(output_json_file, folder ):
    """
    Given a folder of json files, this function will concatenate them all into 1 JSON and returns a dataframe
    Args: 
    output_json_file: name of output json file(in json.gz)
    folder: folder name containing multiple json files
    """
    head = []
    if os.path.exists(folder + '/' + output_json_file):
        os.remove(folder + '/' + output_json_file)
    
    for f in [folder + '/' + f for f in os.listdir(folder) if 'json.gz' in f] :
        with gzip.GzipFile(f, 'r') as fin:    
            json_bytes = fin.read()
            json_str = json_bytes.decode('utf-8')            
            data = json.loads(json_str)
            head += data
    json_str = json.dumps(head) + "\n"               
    json_bytes = json_str.encode('utf-8')            

    with gzip.GzipFile(folder + '/' + output_json_file, 'w') as fout:   
        fout.write(json_bytes)

        
    return pd.read_json(folder + '/' + output_json_file)

def sql_connect(username,password,connection_string,database):
    """
    Connection to mysql database using sql alchemy(python)
    Args:
    username: SQL Username 
    password: SQL Password
    connection_string: IP adress/path(can be localhost)
    database: 
    
    """
    sqlEngine       = create_engine('mysql+pymysql://'+username+':'+password+'@'+connection_string+'/'+database, pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    return dbConnection

def df_to_table(dbConnection,tableName,dataFrame):
    """
    Writes a dataframe into table
    Args:
    dbConnection: Connection using sql_connect function
    tableName: Name of table to write
    dataFrame: dataframe to write
    """
    try:
        frame           = dataFrame.to_sql(tableName, dbConnection, if_exists='fail')
    except ValueError as vx:
        print(vx)
    except Exception as ex:   
        print(ex)
    else:
        print("Table %s created successfully."%tableName)

def preprocess(df):
    """
    Preprocessing of the dataframe. Here are the steps:
    1. Remove duplicates from table using the compound key: ['upc', 'store_number', 'department', 'category', 'name' ]
    2. Price column pre processing: setting all fields with not numbers(190k records had price = nosql) to null and setting the column to float
    3. upc column pre processing: setting all fields with not numbers or empty(190k records had upc = '') to null
    4. store number column pre processing: setting all fields with not numbers or empty(190k records had store_number = '') to null
    5. Return a dataframe with nulls dropped
    
    """
    pd.set_option('display.float_format', lambda x: '%.2f' % x)
    df.drop_duplicates(subset=['upc', 'store_number', 'department', 'category', 'name' ])
    df['price'] = df.price.astype('str').apply(lambda x: np.nan if len(re.findall(r'^[A-Za-z_.]+$', x)) != 0 else x).astype('float64')
    df['upc'] = df.upc.astype('str').apply(lambda x: np.nan if len(re.findall(r'^[A-Za-z_.]+$', x)) != 0 else 
                                             (np.nan if (x == '')  else x)).astype('float64')
    df['store_number'] = df.store_number.astype('str').apply(lambda x: np.nan if len(re.findall(r'^[A-Za-z_.]+$', x)) != 0 else
                                                               (np.nan if (x == '')  else x)).astype('float64')
    return df.dropna()

def read_from_db(database,tablename,dbConnection):
    """
    Read a table from mysql
    """
    
    df = pd.read_sql('select * from ' + database + '.'+ tablename, dbConnection)
    return df

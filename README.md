# Product ETL

## Introduction

Product ETL repository is a simple app to extract data from AWS and perform ETL and provide a serving copy for further querying. This program has been coded in Python and the database used is MySQL. 

## Workflow

Following is the workflow of the process:
![image.png](attachment:image.png)

## Application
A Heroku application has been deployed. The link is: https://flask-product-etl.herokuapp.com/
### Usage 
https://flask-product-etl.herokuapp.com/?upc=10000001317500&store_number=1001 

or

curl "https://flask-product-etl.herokuapp.com/?upc=10000001317500&store_number=1001"

Please note that since it's a get request, it may be blocked by the organization's firewall


## Output

The results(final serving table) has been deployed to clearDB on Heroku. The connection String is:
- Username: b48faf817ccf22
- Password: 1ddc4b76
- Connection String: us-cdbr-east-02.cleardb.com
- Database name: heroku_65bd203f699b466
- Table Name: pmt_tb_products

## RDBMS or Documents?
The bucket had 4940 files with 1000 records each. These files were heavily compressed in .json.gz. After trying csv format (>800 MBs), .json format(>1.4GB), .json.gz format(~350 MBs), below are the points highlighting why MySQL was chosen:
    1. Ease to connect: MySQL connection can be used anywhere in the world.
    2. Performance: Pandas do not perform well with files >300 Mbs and reloading it takes time. Connection and querying MySQL took less than 5 seconds.
    3. It saves a lot of space :)

## Technical Design

The main class in this project is etl.py. This file calls functions from utils.py. etl.py contains 2 classes:

### Class Etl:
Class Etl does all the end to end steps that are involved for the ETL of the pipeline. Class Etl has 1 function which is called **end_to_end** . The following are the functions/steps involved in end_to_end:

1. **download_s3_folder**: This function authenticates credentials from AWS and downloads all the json files into a specified folder. Runtime is 10-15 mins depending on internet connection .The args are:
    - aws_access_key_id
    - aws_secret_access_key
    - region_name
    - bucket_name
    - s3_folder
    - local_dir
    
2. **json_concat**: Once all files are downloaded, it is concatenated into 1 file of format .json.gz(to preserve memory by 67%). Runtime is 3-4 mins (can vary on processing power) .Args are:
    - concat_json_file: Name of file of concatenated json
    - local_dir: Location of concatenated json file
    
3. **sql_connect**: Establishes an SQL connection with the database. Returns a dbconnection object. Args: 
    - db_username
    - db_password
    - db_conn_string 
    - database    

4. **df_to_table**: Inserts table into database, since no pre processing has been done yet, it will be a landing table. Runtime is 2-3 mins as number of rows are large .Args are: 
    - dbConnection: dbConnection established from previous function
    - lmt_table_name: name of table to write 
    - concat_df: name of concatenated df
    
5. **preprocess**: Preprocessing of the dataframe. Runtime is 1-2 mins but can vary on computational power Here are the steps:
    1. Remove duplicates from table using the compound key: ['upc', 'store_number', 'department', 'category', 'name' ]
    2. Price column pre processing: setting all fields with not numbers(190k records had price = nosql) to null and setting the column to float
    3. upc column pre processing: setting all fields with not numbers or empty(190k records had upc = '') to null
    4. store number column pre processing: setting all fields with not numbers or empty(190k records had store_number = '') to null
    5. Return a dataframe with nulls dropped
    
    **Started of with 4.94M rows and the Dataframe after pre processing dropped 570k rows and ended up with 4.37M rows**
    
This dataframe is published to Mysql as a serving table.    
    
### Class QueryProduct:
This class contains function for querying the product. It is called **get_product** and it has the following arguments:
1. upc
2. store_number

Runtime: < 5 seconds


## Usage

The usage is defined in the notebook: **to run.ipynb**. It can be referred to for the following scenarios:

### Scenario 1: Run the whole code end to end
This will download the files, load into the mysql, preprocess and push precoessed data into server. Here are the steps:
1. Import Class ETL
2. Enter the credentials
3. Create an object
4. Run end to end function

### Scenario 2: Run the get_product function only
Assuming that there is already a serving layer table ready and we just need to query for products, here are the steps:
1. Import Class QueryProduct
2. Enter the credentials for database(Credentials entered for Scenario 1 will work perfectly fine)
3. Create an object
4. Run get_product function for given upc and store_number

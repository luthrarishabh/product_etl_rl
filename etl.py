class Etl:
    
    def __init__(self, aws_access_key_id,aws_secret_access_key,region_name,bucket_name, s3_folder, 
                 db_username,db_password,db_conn_string,database,lmt_table_name,pmt_table_name ,local_dir=None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.bucket_name = bucket_name
        self.s3_folder = s3_folder
        self.local_dir = local_dir
        self.db_username = db_username
        self.db_password = db_password
        self.db_conn_string = db_conn_string
        self.database = database
        self.lmt_table_name = lmt_table_name
        self.pmt_table_name = pmt_table_name
        self.concat_json_file = 'concat_json_file.json.gz'
        
    def end_to_end(self):
        import utils
        import pandas as pd
        utils.download_s3_folder(aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key= self.aws_secret_access_key,region_name = self.region_name,
                                 bucket_name = self.bucket_name, s3_folder= self.s3_folder, local_dir = self.local_dir)
        print("Concatenating json files")
        concat_df = utils.json_concat(self.concat_json_file, self.local_dir )
        print("Concatenation done")
        print(concat_df.shape)
        
        print("Creating a Landing table in MySQL ")
        
        dbConnection = utils.sql_connect(self.db_username,self.db_password,self.db_conn_string,self.database)
        utils.df_to_table(dbConnection,self.lmt_table_name,concat_df)
        
        
        print("Cleaning data")
        preprocessed_df = utils.preprocess(concat_df)
        print(preprocessed_df.shape)        
        print("Creating a Serving table in MySQL")
        utils.df_to_table(dbConnection,self.pmt_table_name,preprocessed_df)

    
class QueryProduct:   
    
    def __init__(self,db_username,db_password,db_conn_string,database,table_name):
        self.db_username = db_username
        self.db_password = db_password
        self.db_conn_string = db_conn_string
        self.database = database
        self.table_name = table_name
    
    def get_product(self,upc,store_number):
        import utils
        import pandas as pd
        dbConnection = utils.sql_connect(self.db_username,self.db_password,self.db_conn_string,self.database)
        df = pd.read_sql('select * from '+ self.database + '.' + self.table_name +' where upc = '+upc+' and store_number = ' + store_number , dbConnection)
        return df
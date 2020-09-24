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
        
    def end_to_end(self):
        import utils
        print("Downloading files from s3")
        utils.download_s3_folder(aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key= self.aws_secret_access_key,region_name = self.region_name,
                                 bucket_name = self.bucket_name, s3_folder= self.s3_folder, local_dir = self.local_dir)
        print("Downloaded files from s3")
        print("Concatenating json files")
        concat_df = utils.concat_json_files(self.local_dir)
        print("Concatenation done")
        
        
        print("Creating a Landing table in MySQL ")
        
        dbConnection = utils.sql_connect(self.db_username,self.db_password,self.db_conn_string,self.database)
        utils.df_to_table(dbConnection,self.lmt_table_name,concat_df)
        
        
        print("Cleaning data")
        preprocessed_df = utils.preprocess(concat_df)
        
        print("Creating a Serving table in MySQL")
        utils.df_to_table(dbConnection,self.pmt_table_name,preprocessed_df)
        
        return preprocessed_df
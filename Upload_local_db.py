from sqlalchemy import create_engine, inspect
from data_cleaningt import Datacleaning
import yaml    

class uploader(Datacleaning):
     # Open and read credentials file
    def read_local_db_cred(self,file_name='MRDC.yaml'):
        with open(file_name, 'r') as file:
            file_content=file.read()
            cred=yaml.safe_load(file_content)
        return cred
    #creat sqlalchemy engine
    def init_engine(self):
        cred=self.read_local_db_cred()
        #define connection string 
        db_url=f"postgresql://{cred['user']}:{cred['password']}@{cred['host']}:{cred['port']}/{cred['Database']}"
        # Create database engine and connection
        engine=create_engine(db_url)
        return engine 
    
    
    # connect to data base using sqlalchemy engine 
    def upload_to_local_db(self):
        # Get user data
        user_data=self.clean_user_data()
        #get sqlachemy engine 
        engine=self.init_engine()
       # Define table name 
        table_name='dim_user'
     # Write user data to local database table
        user_data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        
     #upload card detail on local data base 
    def upload_card_details(self):
        engine=self.init_engine()
        card_details=self.clean_card_data()
        table_name='dim_card_details'
        card_details.to_sql(table_name,con=engine,if_exists='replace',index=False)
    #upload store details on local data base 
    def upload_to_database(self):
        engine=self.init_engine()
        store_data=self.clean_store_data()
        table_name='dim_store_details'
        store_data.to_sql(table_name,con=engine,if_exists='replace',index=False)

        
          

obj=uploader()
#obj.upload_to_local_db()
#obj.upload_card_details()
obj.upload_to_database()
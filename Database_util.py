from sqlalchemy import create_engine, inspect
import yaml
class ConnectRDS():
    def read_credentilas(self,file_name='db_creds.yaml'):
        with open(file_name ,'r') as file:
            file_content=file.read()
            cred=yaml.safe_load(file_content)
        return cred
    def init_engine(self):
        creds = self.read_credentilas()
        db_url=f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine
    def list_table(self):
        engine=self.init_engine()
        inspector=inspect(engine)
        table_list=inspector.get_table_names()
        return table_list
    
obj=ConnectRDS()
tables=obj.list_table()
print(tables)

from Database_util import ConnectRDS
import sqlalchemy
import pandas as pd
import tabula
import requests

class Extractor(ConnectRDS):
    def extract(self,table_name):
        engine=self.init_engine()
        query=f"SELECT * FROM {table_name};"
        with engine.connect() as connection:
            data=pd.read_sql(query,connection)
        return data
    '''this function reads multiple tables from pdf file '''
    def read_pdf_tables(self):
        file_path='https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        dfs=tabula.read_pdf(file_path, pages='all', multiple_tables=True)
        return dfs
    '''this function merges multiple data frame'''
    def merge_dataframes(self):
        dfs=self.read_pdf_tables()
        #creat empty data_fram
        merged_df=pd.DataFrame()
        for df in dfs:
            merged_df=pd.concat([merged_df,df],ignore_index=True)
        return merged_df
    #this function extracts number of stores using API
    def list_number_of_stores(self, num_stor_endpoint, header_dict):
        try:
             response = requests.get(num_stor_endpoint, headers=header_dict)

             if response.status_code == 200:
                return response.json()  
             else:
                return f"Error: The request failed with status code {response.status_code}"
        except requests.RequestException as e:
            return f"Error: Request failed - {e}"
    def retrieve_stores_data(self, store_end_point,header_dict, store_number):
        store_data_dict={}
        store_df=pd.DataFrame()
       # store_data={}
        try:
            # Replace the {store_number} placeholder with an actual store number
            for i in range(store_number):
              current_store_end_point = store_end_point.format(store_number=i)
              
              store_data = requests.get(current_store_end_point,headers=header_dict)

              if isinstance(store_data, requests.Response) and store_data.status_code == 200:
                 
                 json_data = store_data.json()  # Extract JSON data from response
                 temp_df = pd.DataFrame([json_data.values()], columns=json_data.keys())  # Convert JSON to DataFrame row
                 store_df = pd.concat([store_df, temp_df], axis=0, ignore_index=True)  # Concatenate to the main DataFrame
                 #return store_data_dict # Assuming the response is in JSON format
              else:
                
                print(f"Failed to retrieve data for store {i}. Status code: {store_data.status_code}")
            return store_df
        except requests.RequestException as e:
            return f"Error: Request failed - {e}"


obj=Extractor()
#result=obj.extract('legacy_users')
#stat=result.info()
#pdf=obj.merge_dataframes()
#print(pdf)
#print(stat)
#print(result)

credintial_yaml='local_db.yaml'
num_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
header_dict = {
    
    "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
}

store_end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

result = obj.list_number_of_stores(num_stores_endpoint, header_dict)
print(result)

# Replace {store_number} with the actual store number
store_number = 450  # Replace this with the actual store number
result_store_data = obj.retrieve_stores_data(store_end_point,header_dict,store_number)
print(result_store_data)

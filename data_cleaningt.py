from Data_extractor import Extractor
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from io import StringIO
import json
import boto3
class Datacleaning(Extractor):
   
    def clean_names(self):
        data=self.extract('legacy_users')
    #replace NULL values with Numpy nan
        data.replace('NULL',np.nan,inplace=True)
        try:
         #filter first and last name
         data.loc[~data['first_name'].astype(str).str.contains(r'^[A-Za-z]',na=False),'first_name']=np.nan
         data.loc[~data['last_name'].astype(str).str.contains(r'^[A-Za-z]',na=False),'last_name']=np.nan
         data.dropna(inplace=True)
         return data
        except Exception as e :
            print(f"Error while handling first and last name: :{e}")
    def clean_country(self,data):
       try:
        #filter country and country code 
        data.loc[~data['country_code'].astype(str).str.contains(r'[A-Za-z]',na=False),'country_code']=np.nan
        data.loc[~data['country'].astype(str).str.contains(r'[A-Za-z]',na=False),'country']=np.nan
        data.dropna(inplace=True)
        return data
       except Exception as e:
          print(f"Error in country field:{e}")
    def clean_date_time(self,data):
       try:
        #filter date
        data['date_of_birth'] = pd.to_datetime(data['date_of_birth'], format='%Y-%m-%d', errors='coerce')
        data.replace('NULL',np.nan,inplace=True)
        data.dropna(inplace=True)
        return data
       except Exception as e:
          print(f"Error in Date time field {e}")
    def clean_duplicates(self,data):
        
        data.drop_duplicates(inplace=True)
        return data
    def clean_user_data(self):
        cleaned_names=self.clean_names()
        cleaned_country=self.clean_country(cleaned_names)
        cleaned_date_time=self.clean_date_time(cleaned_country)
        cleaned_user_data=self.clean_duplicates(cleaned_date_time)
        return cleaned_user_data
    def clean_card_data(self):
       card_data=self.merge_dataframes()
        #clean missing values 
       card_data.replace('NULL',np.nan,inplace=True)
       card_data.dropna(inplace=True)
    # clean date_payment_confirmed column
       card_data=card_data[~card_data['date_payment_confirmed'].astype(str).str.contains(r'[a-zA-Z]')]
    
    # Check if the 'expiry_date' column is full of NaN values
       if card_data['expiry_date'].isnull().all():
        # If it is, remove the 'expiry_date' and 'Unnamed: 0' columns from the DataFrame
          card_data = card_data.drop(['expiry_date', 'Unnamed: 0'], axis=1)
       return card_data
    
    #clean store data
    def clean_store_data(self):
          store_number = 450
          header_dict = {
    
                  "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
                       }

          store_end_point = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

          store_df=self.retrieve_stores_data(store_end_point, header_dict, store_number)
          store_df=pd.DataFrame(store_df)
        
          try:
            if 'lat' in store_df.columns:
             #drop column 'lat' because it is full of NULL values
             store_df = store_df.drop(columns=['lat'])  # Drop the 'lat' column if it exists
             # Additional data cleaning steps if needed
            store_df = store_df.reset_index(drop=True)  # Reset index and drop the old index column
            #clean missing values N/A
            store_df.dropna(inplace=True)
            #clean longitude column datas that contain laters 
            cleaned_df = store_df[~store_df['longitude'].astype(str).str.contains(r'[a-zA-z]')]
            #clean stuff numbers column
            cleaned_data=cleaned_df[~cleaned_df['staff_numbers'].astype(str).str.contains(r'[a-zA-Z]')]
            #cleaned_data['opening_date']=pd.to_datetime(cleaned_data['opening_date'],format='%y-%m-%d',errors='coerce')
            cleaned_datas=cleaned_data[~cleaned_data['opening_date'].astype(str).str.contains(r'[a-zA-Z]')]
            return cleaned_datas
          except Exception as e:
            print(f"Error: while cleaning the data {e}")

    def clean_weight_column(self, df):
        # Define a function to convert the weight values to kilograms
        def convert_to_kg(weight):
            try:
                weight = str(weight).lower()  # Convert to lowercase for uniformity

                # Extract the numeric value from the weight string
                numeric_part = ''.join(filter(lambda x: x.isdigit() or x == '.', weight))
                numeric_value = float(numeric_part) if numeric_part else None  # Convert to float

                # Convert ml to kg using a 1:1 ratio
                if 'kg' in weight:
                    return numeric_value  # No conversion needed for kilograms
                elif 'g' in weight:
                    return numeric_value / 1000  # Convert g to kg
                elif 'ml' in weight:
                    return numeric_value / 1000  # Convert ml to kg

                # If no unit is specified, assume it's in kilograms
                return numeric_value

            except Exception as e:
                print(f"Error occurred: {e}")
                return None  # Return None for any errors encountered

        # Apply the conversion function to the 'weight' column
        df['weight'] = df['weight'].apply(convert_to_kg)

        return df

obj=Datacleaning()
#result=obj.clean_card_data()
#print(result)
#cleaned_store=obj.clean_store_data()
#print(cleaned_store)

#read product details from csv file 
data=pd.read_csv('csv_file.csv')
df_frame=pd.DataFrame(data)
df=df_frame.dropna()
df=df[~df['product_price'].astype(str).str.contains(r'[a-zA-Z]')]
result=obj.clean_weight_column(df)
    

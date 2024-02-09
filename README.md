
![Logo](https://th.bing.com/th/id/OIG2.yx4GK5S7A2FHJiWWACyG?w=1024&h=1024&rs=1&pid=ImgDetMain)




![Static Badge](https://img.shields.io/badge/python-%233776AB?style=flat-square&logo=python-%233776AB&label=VERSION%203.11)  
![Static Badge](https://img.shields.io/badge/pandas-%23150458?style=flat&logo=pandas&label=VERSION%202.2.1)

![Static Badge](https://img.shields.io/badge/postgreSQL-%234169E1?style=flat&logo=postgreSQL&logoColor=black&labelColor=white&color=green)

![Static Badge](https://img.shields.io/badge/amazons3-%23569A31?style=flat&logo=amazons3&logoColor=black&labelColor=white&color=green)

![Static Badge](https://img.shields.io/badge/amazonrds-%23527FFF?style=flat&logo=amazonrds&logoColor=black&labelColor=white&color=green)

# Multinational Retail Data Centralisation



This project aims to centralize and streamline data management processes for a multinational retail company. By integrating various data sources such as AWS RDS(relational database), PDF files, APIs, and AWS S3 buckets, the project facilitates efficient data extraction, transformation, and loading (ETL) operations. The centralized data repository enables improved data analysis, reporting, and decision-making across different business units.
## Features

- **Database Connectivity:** Connects to PostgreSQL databases to extract data from different tables using SQLAlchemy
- **PDF Table Extraction:** Utilizes the Tabula library to extract tabular data from PDF files, enhancing data accessibility.
- **API Integration:** Retrieves data from RESTful APIs to gather real-time information such as store details and product data.
- **AWS S3 Integration:** Downloads files and extracts data from AWS S3 buckets, enabling seamless access to cloud-based resources.
- **Data Cleaning:** Cleans and preprocesses extracted data to ensure consistency, accuracy, and reliability for downstream analysis.
- **Data Upload to Local Database:** Uploads cleaned data to a local PostgreSQL database for centralized storage and easy access.
- **SQL schema set up:** Creation of tables for orders, dimensional    store details, products, users, date times, and card details Alterations for compatibility and optimization.Data transformations such as currency symbol removal, unit conversion, and addition of new columns.Data quality checks and analysis queries.



## components 
1. **ConnectRDS Class:** Handles database connection and table listing functionalities.
2. **Extractor Class:** Inherits from ConnectRDS and provides methods for data extraction from various sources such as databases, PDFs, and APIs.
3. **Data Cleaning Functions:** Includes functions to clean and preprocess extracted data, ensuring data quality and integrity.
4. **Uploader Class:** Inherits from DataExtractor and manages data upload tasks to the local PostgreSQL database.
5. **sql_quiry script:** manages the creation, alteration, and transformation of the database schema for the Retail Data Centralization Project. By ensuring data integrity, optimizing column types, and performing data quality checks, the schema is prepared for efficient data analysis and reporting.

## Usage
1. **Clone Repository:** Clone this repository to your local machine.
```bash
git clone https://github.com/your_username/retail-data-centralization.git
```

2. **Set Up Database Credentials:** Create a YAML file (MRDC.yaml) containing PostgreSQL database credentials in the following format:

```yaml

user: your_username
password: your_password
host: your_host
port: your_port
Database: your_database

```

Ensure you have the following dependencies installed before running the project:

- **SQLAlchemy:** Used for database connection and interaction.
```bash
  pip install sqlalchemy
```
- **Pandas:** Utilized for data manipulation and analysis.
```bash
  pip install pandas
  ```
- **tabula-py:** Necessary for reading tables from PDF files.
 ```bash 
 pip install tabula-py
```
- **Requests:** Used for making HTTP requests to APIs.
```bash 
pip install requests
```
- **Boto3:** Required for interacting with AWS services such as S3.
```bash 
pip install boto3
```
- **NumPy:** Fundamental package for scientific computing with Python.
```bash 
pip install numpy
```
- **psycopg2:** PostgreSQL adapter for Python, if using PostgreSQL as the database backend
```bash
pip install psycopg2
```

## Requirements

- Python 3.x
- PostgreSQL Database
- Access to AWS S3 Bucket
- API Key for External APIs
## Feedback

If you have any feedback, please reach out to me at borcealo7@gmail.com


import pymongo
from pymongo import MongoClient
import mysql.connector as msql
from mysql.connector import error
import pandas as pd
import os
from dotenv import load_dotenv

# Extracting data from mysql database

try:
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="etl_database"
)

# creating a connection to mysql databas

if connection.is_connected():
    db_Info = connection.get_server_info() 
    print("Connected to MySQL Server version ", db_Info) # getting the 
    server info
    cursor = connection.cursor() 
    cursor.execute("select database();") # selecting the database diamond
    record = cursor.fetchone()
    print("You're connected to database: ", record)
    mycursor = connection.cursor()

# executing the query to fetch all record from diamond record

    mycursor.execute("SELECT * from diamond_record") 
    table_rows = mycursor.fetchall()

except Error as e:
    print("Error while connecting to MySQL", e)
 
finally:
    if connection.is_connected():
       cursor.close()
       connection.close()
       print('mysql connection is closed')

# Transform data using pandas dataframe

# Creating dataframe

df = pd.DataFrame(table_rows,columns=["upc","title",
"product_class","index_id","shape","price","carat","color","cut","depth","girdle"])

# Dropping unwanted fields

new_df = df.drop(columns=["product_class","index_id","cut"])
 
print(new_df)


# Loading data to sink (__mongodb__)

# Making connection to Mongocloud

# Load environment variables from .env file
load_dotenv()

# Get the MongoDB connection string from environment variables

mongo_uri = os.environ.get('MONGO_URI')

cluster =
pymongo.MongoClient("mongo_uri")

# creating collection testdb 

db = cluster["testdb"]
collection = db["test"]
 
# Inserting values to table test 

x = collection.insert_many(new_df.to_dict('records')) #myresult comes from mysql cursor
print(len(x.inserted_ids))

# creating collection testdb 

db = cluster["testdb"]
collection = db["test"]
 
# Inserting values to table test 

x = collection.insert_many(new_df.to_dict('records')) #myresult comes from mysql cursor
print(len(x.inserted_ids))

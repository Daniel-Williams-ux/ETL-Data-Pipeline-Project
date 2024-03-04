from pymongo import MongoClient
import mysql.connector as msql
import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime

# connect to MySQL database
def connect_to_mysql():
    connection = msql.connect(
        host="localhost",
        user="root",
        password="root",
        database="etl_database"
    )
    print("Connected to MySQL database")
    return connection

# fetch diamond records from MySQL
def fetch_diamond_records(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM diamond_record LIMIT 10")
    records = cursor.fetchall()
    return records

# Transform data using pandas dataframe

# Creating dataframe
df = pd.DataFrame(table_rows, columns=["upc", "title", "product_class", "index_id", "shape", "price", "carat", "color", "cut", "depth", "girdle"])

# Dropping unwanted fields
new_df = df.drop(columns=["product_class", "index_id", "cut"])
 
print(new_df)

# connect to MongoDB
def connect_to_mongodb():
    load_dotenv()
    mongo_uri = os.environ.get('MONGO_URI')
    cluster = MongoClient(mongo_uri)
    print("Connected to MongoDB")
    return cluster

# Function to insert records into MongoDB
def insert_records_to_mongodb(cluster, records):
    db = cluster["ETLdb"]
    collection = db["etl"]
    current_time = datetime.now()
    records_with_time = [{"insertedAt": current_time, **record} for record in records]
    x = collection.insert_many(records_with_time)
    print("Inserted", len(x.inserted_ids), "records into MongoDB")

# Main function
def main():
    try:
        # Connect to MySQL database
        connection = connect_to_mysql()
        # Fetch diamond records from MySQL
        records = fetch_diamond_records(connection)
        # Connect to MongoDB
        cluster = connect_to_mongodb()
        # Insert records into MongoDB
        insert_records_to_mongodb(cluster, transformed_data.to_dict('records'))
        cluster.close()
        connection.close()
        print("Process completed.")
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()


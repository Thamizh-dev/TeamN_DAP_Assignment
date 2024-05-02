import subprocess

def install_package(package):
    subprocess.check_call(["python", '-m', 'pip', 'install', package])

# Install pymongo
install_package('pymongo')

import subprocess

def install_package(package):
    subprocess.check_call(["python", '-m', 'pip', 'install', package])

# Install xmltodict
install_package('xmltodict')

# After installation, import the module
import xmltodict


# After installation, import the module
from pymongo import MongoClient
import xml.etree.ElementTree as ET
from pymongo import MongoClient,errors 
import xmltodict

# Function to extract data from XML file
def extract_data(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()
    return root

# Function to transform XML data into a list of dictionaries
def transform_data(root):
    data = []
    for item in root.findall('item'):  # Assuming 'item' is the tag containing your data
        record = {}
        for child in item:
            record[child.tag] = child.text
        data.append(record)
    return data

# Function to load data into MongoDB
def load_data_to_mongodb(data, database_name, collection_name):
    client = MongoClient('mongodb://localhost:27017/')
    db = client[database_name]
    collection = db[collection_name]
    collection.insert_many(data)
    client.close()

# Main function to orchestrate ETL process
def main():
    # Extract data from XML file
    root = extract_data("C:/Data Analytics/Database Analytics/2010 to 2019 crime.xml")

    # Transform XML data
    transformed_data = transform_data(root)

    # Load data into MongoDB
    load_data_to_mongodb(transformed_data, 'mydatabase', 'mycollection')

if __name__ == "__main__":
    main()
    

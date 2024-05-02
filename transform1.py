import pandas as pd
from pymongo import MongoClient
import psycopg2
import Dagster 
from sqlalchemy import create_engine
import warnings
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
warnings.filterwarnings("ignore", message="Could not infer format")

def transform_1():

    try:
        # Accessing MongoDB client
        client = MongoClient('mongodb://localhost:27017/')
        # Selecting the database
        db = client['crimes_db']
        # Select the collection within the database
        collection = db['arrest_collection_db']
        # Convert entire collection to Pandas dataframe
        rawDataset1 = pd.DataFrame(list(collection.find()))
        print("Dataset-1 has been imported successfully")
    except Exception as e:
        print(f"An error occurred: {e}")

    # Removing unnecessary columns
    removeddata = rawDataset1.drop(['_id','row','report_type','time','descent_cd','chrg_grp_cd','grp_description',
                                    'arst_typ_cd','charge','dispo_desc','lat','lon','location_1','bkg_date',
                                    'bkg_time','bkg_location','bkg_loc_cd'], axis=1)

    # Fill NaN values with 'Nil'
    arrest_df = removeddata.fillna(value='Nil')


        # Extract year from 'Date Rptd' and 'Arrest Date'
    arrest_df['Year'] = pd.to_datetime(arrest_df['arst_date'], errors='coerce').dt.year
    arrest_df.drop(['arst_date'], axis=1, inplace=True)
    # Assuming your DataFrame is named df_crimes_2020_to_present

    # Combine 'LOCATION' and 'cross street' columns into a new column named 'Location'
    arrest_df['Location'] = arrest_df['location'] + ', ' + arrest_df['crsst']

    # Drop the original 'LOCATION' and 'cross street' columns
    arrest_df.drop(['location', 'crsst'], axis=1, inplace=True)
    print("cleaned data: ",arrest_df['Location'])
    return arrest_df


arrest_df= transform_1()
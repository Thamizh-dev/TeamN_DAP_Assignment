
def transform_2():
    try:
        # Accessing MongoDB client
        client = MongoClient('mongodb://localhost:27017/')
            # Selecting the database
        db = client['crimes_db']
            # Select the collection within the database
        collection = db['crimes_2013to2019_db']
            # Convert entire collection to Pandas dataframe
        rawDataset2 = pd.DataFrame(list(collection.find()))
        print("Dataset-2 has been imported successfully")
    
    except Exception as e:
        
        print(f"An error occurred: {e}")

        datainfo=rawDataset2.info()
        print(datainfo)
        #removing the unnecessary columns from the rawDataset2 dataframe using drop function 
        #and saving it in another dataframe called dataset2_remove
        removeddata=rawDataset2.drop(['_id','DATE OCC','TIME OCC','Part 1-2','Mocodes','Vict Descent','Premis Cd','Premis Desc','Weapon Used Cd','Weapon Desc','Status','Status Desc','Crm Cd 1','Crm Cd 2','Crm Cd 3','Crm Cd 4','LAT','LON'],axis=1)

        # Fill NaN values with 'Nil'
        crime_2013to2019_df = removeddata.fillna(value='Nil')

        # Extract year from 'Date Rptd' and 'Arrest Date'
        crime_2013to2019_df['Year'] = pd.to_datetime(crime_2013to2019_df['Date Rptd'], errors='coerce').dt.year
        crime_2013to2019_df.drop(['Date Rptd'], axis=1, inplace=True)

        # Combine 'LOCATION' and 'cross street' columns into a new column named 'Location'
        crime_2013to2019_df['Location'] = crime_2013to2019_df['LOCATION'] + ', ' + crime_2013to2019_df['Cross Street']

        # Drop the original 'LOCATION' and 'cross street' columns
        crime_2013to2019_df.drop(['LOCATION', 'Cross Street'], axis=1, inplace=True)
        print("cleaned data: ",crime_2013to2019_df['Location'])
        return crime_2013to2019_df

crime_2013to2019_df = transform_2()
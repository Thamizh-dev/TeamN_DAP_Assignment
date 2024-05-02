def transform_3():
    try:
        # Accessing MongoDB client
        client = MongoClient('mongodb://localhost:27017/')
        # Selecting the database
        db = client['crimes_db']
        # Select the collection within the database
        collection = db['crimes_2020topresent_collection_db']
        # Convert entire collection to Pandas dataframe
        rawDataset3 = pd.DataFrame(list(collection.find()))
        print("Dataset-3 has been imported successfully")
    except Exception as e:
        print(f"An error occurred: {e}")

    datainfo=rawDataset3.info()
    print(datainfo)
    #removing the unnecessary columns from the rawDataset3 dataframe using drop function 
    removeddata=rawDataset3.drop(['_id','row','date_occ','time_occ','part_1_2','vict_descent','premis_cd','premis_desc','status','status_desc','crm_cd_1','crm_cd_2','lat','lon','mocodes','weapon_used_cd','weapon_desc','crm_cd_3','crm_cd_4'],axis=1)

    crime_2020topresent_df = removeddata.fillna(value='Nil')
    nullvalues = crime_2020topresent_df.isnull().sum()

    print(nullvalues)

        # Extract year from 'Date Rptd' and 'Arrest Date'
    crime_2020topresent_df['Year'] = pd.to_datetime(crime_2020topresent_df['date_rptd'], errors='coerce').dt.year
    crime_2020topresent_df.drop(['date_rptd'], axis=1, inplace=True)
    # Assuming your DataFrame is named df_crimes_2020_to_present

    # Combine 'LOCATION' and 'cross street' columns into a new column named 'Location'
    crime_2020topresent_df['Location'] = crime_2020topresent_df['location'] + ', ' + crime_2020topresent_df['cross_street']

    # Drop the original 'LOCATION' and 'cross street' columns
    crime_2020topresent_df.drop(['location', 'cross_street'], axis=1, inplace=True)
    print("cleaned data: ",crime_2020topresent_df['Location'])
    return crime_2020topresent_df

crime_2020topresent_df = transform_3()
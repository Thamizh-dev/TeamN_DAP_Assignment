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


arrest_df= transform_1()
crime_2013to2019_df = transform_2()
crime_2020topresent_df = transform_3()

merged_df = pd.merge(crime_2013to2019_df, crime_2020topresent_df, on='Year', suffixes=('_2013to2019_df', '_2020topresent_df'), how='outer')
print('merged df: ',merged_df)

# Count the occurrences of each crime type
crime_counts = merged_df['Crm Cd Desc'].value_counts()
# Select the top 10 most common crime types
top_10_crimes = crime_counts.head(10).index.tolist()

# Filter the dataset to include only those crime types
filtered_df = merged_df[merged_df['Crm Cd Desc'].isin(top_10_crimes)]
print("Filtered DataFrame with the top 10 most common crimes:")
print(filtered_df)

# for crimes2020topresent
crime_counts1 = crime_2020topresent_df['crm_cd_desc'].value_counts()
# Select the top 10 most common crime types
top_10_crimes1 = crime_counts1.head(10).index.tolist()

# Filter the dataset to include only those crime types
filtered_df2 = crime_2020topresent_df[crime_2020topresent_df['crm_cd_desc'].isin(top_10_crimes1)]
print("Filtered DataFrame with the top 10 most common crimes:")
print(filtered_df2)
# Rename the columns
merged_df.rename(columns={'AREA': 'area','AREA NAME':'area_name','Rpt Dist No':'rpt_dist_no',
                          'Crm Cd':'crm_cd','Crm Cd Desc':'crm_cd_desc','Vict Age':'vict_age','Vict Sex':'vict_sex'}, inplace=True)

print('Renamed DataFrame: ', merged_df)

 # to connect with postgre Sql

try:
    conn = psycopg2.connect(user='postgres',
    password='2112',
    host='localhost',
    port='5432',
    database='crimes_dap')
    print("Test connection succeeded")
except Exception as e:
    print(f"Test connection failed: {e}")

def loadDatasetToPostgresql():
    try:
# Create database connection
        engine = create_engine('postgresql://postgres:2112@localhost:5432/postgres')
        postgreSQLConnection = engine.connect()
            
            # Load data to the database
        merged_df.to_sql('crimes_2013topresent_data', engine, if_exists='replace', method='multi')
        print(" merged_df Successfully loaded in the Postgresql database")

        arrest_df.to_sql('arrest_data', engine, if_exists='replace', method='multi')
        print(" merged_df Successfully loaded in the Postgresql database")


        
    except ValueError as ve:
        print(ve)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        
    finally:
            # Close database connection
        postgreSQLConnection.close()

loadDatasetToPostgresql()


# Function for Exploratory Data Analysis

def perform_eda(data):
    # Summary statistics
    print("Summary Statistics:")
    print(data.describe())
    
    # Plotting crime types

    # Distribution of top 10 Crime by count plot
    sns.set_style("whitegrid")
    plt.figure(figsize=(10, 6))
    ax=sns.countplot(y='Crm Cd Desc', data=data, order=data['Crm Cd Desc'].value_counts().index, palette="viridis")
    ax.set_title('Distribution of Crime Types',fontsize=16)
    ax.set_xlabel('Count', fontsize=12)
    ax.set_ylabel('Crime Type', fontsize=12)
    ax.set_xticklabels(ax.get_xticklabels(),rotation=45, ha='right')  
    ax.tick_params(axis='both', which='major', labelsize=12)  
    ax.grid(axis='x', linestyle='--', alpha=0.7)
    sns.despine()
    plt.tight_layout() 
    plt.show()
    
    plt.figure(figsize=(10, 6))
    ax = sns.boxplot(y='Vict Age', data=data, notch=False, color='skyblue')
    # Add number of observations
    def add_n_obs(df, y):
        n_obs = df[y].count()
        ax.text(0, df[y].median() * 1.01, "#obs : " + str(n_obs), horizontalalignment='center', 
                fontdict={'size': 14}, color='white')
    add_n_obs(data, y='Vict Age')    
    ax.set_title('Age Distribution of Victims', fontsize=16)
    ax.set_xlabel('Age', fontsize=12)
    ax.set_ylabel('Count', fontsize=12) 
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')    
    ax.tick_params(axis='both', which='major', labelsize=10)   
    sns.despine()    
    plt.tight_layout() 
    plt.show()
    colors = ['skyblue', 'salmon']

# Plotting gender distribution as a bar plot
    plt.figure(figsize=(8, 6))
    gender_counts = data['Vict Sex'].value_counts()
    sns.barplot(x=gender_counts.index, y=gender_counts.values, palette=colors)
    plt.title('Gender Distribution of Victims')
    plt.xlabel('Gender')
    plt.ylabel('Count')
    plt.xticks(rotation=0)
    for index, value in enumerate(gender_counts):
        plt.text(index, value + 10, str(value), ha='center', va='bottom', fontsize=10)
    plt.gca().set_facecolor('lightgray')
    sns.despine()
    plt.tight_layout()
    plt.show()


    # Plotting crime occurrences by area
    plt.figure(figsize=(10, 6))
    sns.countplot(y='AREA NAME', data=data, order=data['AREA NAME'].value_counts().index)
    plt.title('Crime Occurrences by Area')
    plt.xlabel('Count')
    plt.ylabel('Area')
    plt.xticks(rotation=45, ha='right')  
    sns.despine()
    plt.tight_layout() 
    plt.show()

# Function for year-wise crime comparison
def year_wise_comparison1(data):
    top_3_years = data['Year'].value_counts().head(3).index
    
    # Filtering the data for the top 3 years
    filtered_data1 = data[data['Year'].isin(top_3_years)]
    pivot_table = filtered_data1.groupby(['Year', 'Crm Cd Desc']).size().unstack(fill_value=0)
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_table, cmap='RdYlGn', annot=True, fmt='d')
    plt.title('Year-wise Crime Comparison 2013-2015')
    plt.xlabel('Crime Type')
    plt.ylabel('Year')
    plt.xticks(rotation=45, ha='right')  
    sns.despine()
    plt.tight_layout() 
    plt.show()

def year_wise_comparison2(data):
    top_3_years = data['Year'].value_counts().head(3).index
    
    # Filtering  the data for the top 3 years
    filtered_data2 = data[data['Year'].isin(top_3_years)]
    pivot_table = filtered_data2.groupby(['Year', 'crm_cd_desc']).size().unstack(fill_value=0)
    plt.figure(figsize=(10, 6))
    sns.heatmap(pivot_table, cmap='RdYlGn', annot=True, fmt='d')
    plt.title('Year-wise Crime Comparison 2021-2023')
    plt.xlabel('Crime Type')
    plt.ylabel('Year')
    plt.xticks(rotation=45, ha='right')  
    sns.despine()
    plt.tight_layout() 
    plt.show()

# Calling the function to display the year-wise comparison heatmap
def perform_age_comparision_arrest(data):
    data['age'] = pd.to_numeric(data['age'], errors='coerce')
    plt.figure(figsize=(10, 6))
    sns.boxplot(y='age', data=data, notch=False)

# Adding the Observation count inside boxplot 
    def add_n_obs(df, y):
        n_obs = df[y].count()
        plt.text(0, df[y].median() * 1.01, "#obs : " + str(n_obs), horizontalalignment='center', fontdict={'size': 14}, color='white')
    add_n_obs(data.dropna(), y='age')    
    plt.title('Age Distribution of arrested people', fontsize=16)
    plt.xlabel('Age', fontsize=12)
    plt.ylabel('Count', fontsize=12)
    plt.xticks(rotation=45, ha='right')  
    plt.tight_layout() 
    plt.show()

# Function for K-means clustering
def kmeans_clustering(data):
    features = ['Year', 'Vict Age', 'Vict Sex', 'Crm Cd Desc']

    data_encoded = pd.get_dummies(data[features])
    scaler = StandardScaler()
    scaled_data = scaler.fit_transform(data_encoded)
    inertia = []
    for n_clusters in range(1, 11):
        kmeans = KMeans(n_clusters=n_clusters, random_state=42)
        kmeans.fit(scaled_data)
        inertia.append(kmeans.inertia_)

    plt.figure(figsize=(8, 6))
    plt.plot(range(1, 11), inertia, marker='o', color='orange')
    plt.title('Elbow Method for Optimal k')
    plt.xlabel('Number of Clusters')
    plt.ylabel('Inertia')
    plt.show()

    # Fit K-means with optimal k
    kmeans = KMeans(n_clusters=3, random_state=42)
    kmeans.fit(scaled_data)

    # Adding cluster labels to original data
    data['Cluster'] = kmeans.labels_
    plt.figure(figsize=(10, 6))
    sns.scatterplot(x='Vict Sex', y='Year', hue='Cluster', data=data, palette='Set2', alpha=0.6)
    plt.title('Crime Involvement by Gender')
    plt.xlabel('Gender')
    plt.ylabel('Count')
    plt.xticks(fontsize=12)  
    plt.yticks(fontsize=12)  
    plt.legend(title='Cluster', fontsize=12) 
    plt.tight_layout()
    plt.show()

def plot_time_series(data):
    # Plotting trend of 'Year from 2013 to 2019 by line plot' 
    plt.figure(figsize=(12, 6))
    sns.lineplot(x='Year', y=data.index, data=data, color='deeppink')
    plt.title('Trend of Year')
    plt.xlabel('Year')
    plt.ylabel('Count')
    plt.show()

    # Plotting trend of 'Vict Sex' over time
    plt.figure(figsize=(12, 6))
    sns.countplot(x='Year', hue='Vict Sex', data=data, color='orange')
    plt.title('Trend of Victim Sex over Time')
    plt.xlabel('Year')
    plt.ylabel('Count')

def plot_time_series1(data):
    # Plotting trend of 'Year 2020 to 2024 by line plot' 
    plt.figure(figsize=(12, 6))
    sns.lineplot(x='Year', y=data.index, data=data, color='purple')
    plt.title('Trend of Year')
    plt.xlabel('Year')
    plt.ylabel('Count')
    plt.show()

    # Plotting trend of 'Vict Sex' over time
    plt.figure(figsize=(12, 6))
    sns.countplot(x='Year', hue='vict_sex', data=data, color='green')
    plt.title('Trend of Victim Sex over Time')
    plt.xlabel('Year')
    plt.ylabel('Count')
    plt.show()        


# Perform EDA
print("### Exploratory Data Analysis ###")
perform_eda(filtered_df)

# Year-wise crime comparison
print("\n### Year-wise Crime Comparison ###")
year_wise_comparison1(filtered_df)

print("\n### Year-wise Crime Comparison ###")
year_wise_comparison2(filtered_df2)

# arrest-people age comparison
print("\n###  arrest-people age comparison ###")
# perform_age_comparision_arrest(arrest_df)

# K-means clustering
print("\n### K-means Clustering ###")
kmeans_clustering(crime_2013to2019_df)
plot_time_series(crime_2013to2019_df)
plot_time_series1(crime_2020topresent_df)

# # Call the function with your data
# plot_age_trend(crime_2013to2019_df)
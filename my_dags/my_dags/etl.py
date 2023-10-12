import requests
import pandas as pd
import json
import logging
import mysql.connector

from transformations import delete_column, lower_case, delete_duplicated_id, delete_duplicated_by_valence, duration_transformation, drop_transformation
from transformations import transformation_parenthesis, transformation_first, transformation_role, transformation_with_workers, transformation_with_nominee, transformation_drop_nulls, transformation_rename, transformation_drop_columns, lower, filter_only_songs
from drive_connect import upload_csv

def extract():
    csv = "/root/workshop2/my_dags/spotify_dataset.csv"
    df_spotify = pd.read_csv(csv)
    logging.info(f"Columns are: {df_spotify.columns}")

    json_data = df_spotify.to_json()
    return json_data

def transform(json_data):
    str_data = json_data[0]
    print("data coming from extract:", json_data)
    print("data type is: ", type(json_data))

    json_data = json.loads(str_data)
    df_spotify= pd.DataFrame(json_data)
    #df_spotify = pd.json_normalize(data=json_data)
    logging.info(f"data is: {df_spotify}")
    logging.info(f"Columns are: {df_spotify.columns}")

    df_spotify = delete_column(df_spotify)
    df_spotify = lower_case(df_spotify)
    df_spotify = delete_duplicated_id(df_spotify)
    df_spotify = delete_duplicated_by_valence(df_spotify)
    df_spotify = duration_transformation(df_spotify)
    df_spotify = drop_transformation(df_spotify)

    json_data = df_spotify.to_json()

    return json_data


def extract_sql():

    with open('/root/workshop2/my_dags/config_db.json') as config_json:
        config = json.load(config_json)
    conx = mysql.connector.connect(**config) 

    mycursor = conx.cursor()

    all_info = "SELECT * from grammys"
    mycursor.execute(all_info)

    results = mycursor.fetchall()

    mycursor.close()

    df_grammys = pd.DataFrame(results, columns=['id', 'year', 'title', 'published_at', 'updated_at', 'category', 'nominee', 'artist', 'workers', 'img', 'winner'])

    logging.info(f"data is: {df_grammys}")
    logging.info(f"Columns are: {df_grammys.columns}")

    json_data = df_grammys.to_json()


    return json_data


def transform_sql(json_data):
    str_data = json_data
    print("data coming from extract:", json_data)
    print("data type is: ", type(json_data))

    json_data = json.loads(str_data)
    df_grammys= pd.DataFrame(json_data)
    #df_spotify = pd.json_normalize(data=json_data)
    logging.info(f"data is: {df_grammys}")
    logging.info(f"Columns are: {df_grammys.columns}")

    df_grammys = transformation_parenthesis(df_grammys)
    df_grammys = transformation_first(df_grammys)
    df_grammys = transformation_role(df_grammys)
    df_grammys = transformation_with_workers(df_grammys)
    df_grammys = transformation_with_nominee(df_grammys)
    df_grammys = transformation_drop_nulls(df_grammys)
    df_grammys = transformation_rename(df_grammys)
    df_grammys = transformation_drop_columns(df_grammys)
    df_grammys= lower(df_grammys)
    df_grammys= filter_only_songs(df_grammys)

    json_data = df_grammys.to_json()

    return json_data



def merge(data1, data2):

    print("data coming from extract:", data1)
    print("data type is: ", type(data1))

    print("data coming from extract:", data2)
    print("data type is: ", type(data2))

    str_data1 = data1
    str_data2 = data2

    json_data1 = json.loads(str_data1)
    json_data2 = json.loads(str_data2)

    df_spotify= pd.DataFrame(json_data1)
    df_grammys= pd.DataFrame(json_data2)

    
    df_merged = df_spotify.merge(df_grammys, how='left', left_on=['track_name', 'artists'], right_on=['nominee', 'artist'])
    df_merged= df_merged.drop(columns=['nominee'])
    df_merged= df_merged.drop(columns=['artist'])
    
    df_merged['was_nominated'].fillna(0, inplace=True)
    json_data = df_merged.to_json()

    return json_data

def load (json_data):
    print("data coming from extract:", json_data)
    print("data type is: ", type(json_data))

    str_data = json_data
    #str_data = json_data[0]
    json_data = json.loads(str_data)
    songs= pd.DataFrame(json_data)

    with open('/root/workshop2/my_dags/config_db.json') as config_json:
        config = json.load(config_json)
    conx = mysql.connector.connect(**config) 

    mycursor = conx.cursor()

    mycursor.execute("CREATE TABLE IF NOT EXISTS songs (id INT AUTO_INCREMENT PRIMARY KEY, artists VARCHAR(1000), track_name VARCHAR(1000), duration_min FLOAT, explicit boolean, danceability FLOAT, energy FLOAT, speechiness FLOAT, instrumentalness FLOAT, valence FLOAT, year INT, title VARCHAR(1000), category VARCHAR(1000), was_nominated boolean)")
    
    for index, row in songs.iterrows():
                  
        values = [row['artists'], row['track_name'], row['duration_min'], row['explicit'], row['danceability'], row['energy'], row['speechiness'], row['instrumentalness'], row['valence'], row['year'], row['title'], row['category'], row['was_nominated']]
    
        # From NaN to None
        values = [None if pd.isna(value) else value for value in values]


        query = "INSERT INTO songs (artists, track_name, duration_min, explicit, danceability, energy, speechiness, instrumentalness, valence, year, title, category, was_nominated) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

        mycursor.execute(query, values)

    songs.to_csv("songs.csv")
    conx.commit()

    return json_data

def store(json_data):

    print("data coming from extract:", json_data)
    print("data type is: ", type(json_data))
    
    #str_data = json_data
    #json_data = json.loads(str_data)
    
    logging.info(f"data is {json_data}")


    upload_csv("songs.csv","1gq1Ih6mCI2_EgKDh5yV2LUKSqap9x2v6")    
    logging.info( f"Airflow workflow completed for workshop2!")

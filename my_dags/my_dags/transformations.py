import pandas as pd
import json
import logging
import random
import re


def delete_column(df):
    # Transformation 1: delete column unnamed:0
    df = df.drop(df.columns[0], axis=1)
    return df

def lower_case(df):
    # Transformation 2: transforming track in lower case
    df['track_name'] = df['track_name'].str.lower()
    return df

def delete_duplicated_id(df):
    # Transformation 3: deleting data with duplicated track_id
    unique_spotify = df.groupby('track_id').apply(lambda group: group.sample(n=1, random_state=random.seed())).reset_index(drop=True)
    return unique_spotify

def delete_duplicated_by_valence(df):
    # Transformation 4: finding songs with maximum valence
    songs = df.groupby(['track_name', 'artists'])['valence'].idxmax()
    final_spotify = df.loc[songs]
    return final_spotify

def duration_transformation(df):
    # Transformation 5: form miliseconds to min rounded
    df['duration_ms'] = df['duration_ms'] / 60000
    df['duration_ms'] = df['duration_ms'].round(2)
    df.rename(columns={"duration_ms": "duration_min"}, inplace=True)
    logging.info(f"data transformed is: {df} with shape: {df.shape}")
    return df

def drop_transformation(df):
    spotify =df.drop(['track_id', 'popularity', 'album_name', 'key', 'loudness', 'mode', 'acousticness', 'liveness', 'tempo', 'time_signature', 'track_genre'], axis=1)
    
    return spotify

def transformation_parenthesis(df):
    def extraer_artista_principal(row):
        if pd.isna(row['artist']) and not pd.isna(row['workers']):
            match = re.search(r'\((.*?)\)', row['workers'])
            if match:
                return match.group(1)
        return row['artist']
    
    df['artist'] = df.apply(extraer_artista_principal, axis=1)
    return df

def transformation_first(df):
    condicion = df['artist'].isnull() & ~df['workers'].isnull()
    df.loc[condicion, 'artist'] = df.loc[condicion, 'workers'].apply(lambda x: x.split(';')[0] if ';' in x else None)
    return df

def transformation_role(df):
    condicion1 = df['artist'].isnull() & ~df['workers'].isnull()
    df.loc[condicion1, 'artist'] = df.loc[condicion1, 'workers'].apply(lambda x: x.split(',')[0] if ',' in x else None)
    return df

def transformation_with_workers(df):
    df.loc[df['artist'].isnull(), 'artist'] = df.loc[df['artist'].isnull(), 'workers']
    return df

def transformation_with_nominee(df):
    df['category'] = df['category'].str.lower()
    condicion = df['artist'].isnull() & df['workers'].isnull() & df['category'].str.contains('best .* artist', case=False, regex=True)
    df.loc[condicion, 'artist'] = df.loc[condicion, 'nominee']
    return df

def transformation_drop_nulls(df):
    df_final = df.dropna(subset=['artist'])
    return df_final

def transformation_rename(df):
    df_final = df.rename(columns={'winner': 'was_nominated'})
    return df_final

def transformation_drop_columns(df):
    df_final = df.drop(['img', 'published_at', 'updated_at', 'workers', 'id'], axis=1)
    return df_final

def lower(df):
    df['nominee'] = df['nominee'].str.lower()

    return df

def filter_only_songs(df):
    df_grammys= df
    #take only nominees that are songs.
    grammy_df_filtered = df_grammys[~(df_grammys['category'].str.contains('album', case=False) | df_grammys['category'].str.contains('artist', case=False))]

    return grammy_df_filtered

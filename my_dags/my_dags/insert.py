import pandas as pd
import mysql.connector
import json


csv_file = "data/the_grammy_awards.csv"
df = pd.read_csv(csv_file)


with open('config_db.json') as config_json:
    config = json.load(config_json)

conx = mysql.connector.connect(**config)

# Crear la tabla 
create_table_query = """
CREATE TABLE IF NOT EXISTS grammys (
    id INT AUTO_INCREMENT PRIMARY KEY,
    year INT,
    title VARCHAR(1000),
    published_at DATETIME,
    updated_at DATETIME,
    category VARCHAR(1000),
    nominee VARCHAR(1000),
    artist VARCHAR(1000),
    workers VARCHAR(1000),
    img VARCHAR(1000),
    winner BOOLEAN
)
"""
mycursor = conx.cursor()
mycursor.execute(create_table_query)
mycursor.close()


# Insertar datos desde el DataFrame en la tabla
mycursor = conx.cursor()
for index, row in df.iterrows():
    values = [row['year'], row['title'], row['published_at'], row['updated_at'], row['category'], row['nominee'], row['artist'], row['workers'], row['img'], row['winner']]
    values = [None if pd.isna(value) else value for value in values]
    query = "INSERT INTO grammys (year, title, published_at, updated_at, category, nominee, artist, workers, img, winner) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.execute(query, values)

# Realizar la confirmación de la transacción y cerrar la conexión
conx.commit()
conx.close()



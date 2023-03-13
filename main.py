import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///playlist.sqlite"
USER_ID = "0800anti"
TOKEN = "BQB_Q9BSoLLjSZ0uoFSh4MimBBy84yuA-5EPykavXfdKeWXKOHNDshmkUP7iFoKjWCPpCR5PTcqsXXo741MogxwcgmUiVTQxPF0QqerKGJ4HFurr8sPiFHq7Su5syZqKGrD6IVcTJckOIQagz7Wyj2DDTfPIqQFKsIikEQn9OlzPmCQ"

def chequear_validez_data(df: pd.DataFrame) -> bool:
   # Chequear si el df vino vacio
    if df.empty:
        print("No hubo canciones descargadas. Terminando ejecución")
        return False
   
    #chequeo de claves primarias
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Problemas con chequeo de PK")
   
   #Chequear nulos
    if df.isnull().values.any():
        raise Exception("Valores nulos encontrados")
   
   #Chequear que la data pertenezca solo a ayer
   ## yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    #timestamps = df["timestamp"].tolist()
    #for timestamp in timestamps:
      #if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
        # raise Exception("Al menos 1 de las canciones no es de ayer")
      #return True

if __name__ == "__main__":

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }
# Convertir tiempo a unix timestamp en milisegundos
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=30)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
#Descargar todas las canciones escuchadas en los últimos 30 dias
    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])

    print(song_df)
  
  #Validar
    if chequear_validez_data(song_df):
        print("Data válida y lista para cargar")

#Cargar
engine = sqlalchemy.create_engine(DATABASE_LOCATION)
conn = sqlite3.connect('my_played_tracks.sqlite')
cursor = conn.cursor()

sql_query = """
CREATE TABLE IF NOT EXISTS my_played_tracks(
song_name VARCHAR(200),
artist_name VARCHAR(200),
played_at VARCHAR(200),
timestamp VARCHAR(200),
CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
)
"""
cursor.execute(sql_query)
print("Abriendo database de forma exitosa")

try:
    song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
except:
    print("Data ya existe en la database")

conn.close()
print("cerrar data")
    



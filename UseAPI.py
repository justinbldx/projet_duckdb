import duckdb
import requests
import pandas as pd

# Recuperer les donnees depuis l'API
url = "https://api.open-meteo.com/v1/forecast?latitude=48.8566&longitude=2.3522&hourly=temperature_2m"
response = requests.get(url)
data = response.json()

# Extraire la partie hourly dans un dataframe
df = pd.DataFrame(data['hourly'])

# Se connecter à DuckDB (en mémoire)
con = duckdb.connect(database=':memory:')

# Charger le dataframe dans DuckDB
con.register('meteo_paris', df)

# Faire des requêtes SQL sur les données
#   Afficher les 10 premières heures
print(con.execute("""
    SELECT time AS heure, temperature_2m AS temperature
    FROM meteo_paris
    WHERE temperature_2m IS NOT NULL
    ORDER BY time
    LIMIT 10
""").fetchdf())

#  Calculer la température moyenne
print(con.execute("""
    SELECT AVG(temperature_2m) AS temperature_moyenne
    FROM meteo_paris
    WHERE temperature_2m IS NOT NULL
""").fetchdf())

print(con.execute("""
    SELECT *
    FROM read_json(
    'https://api.open-meteo.com/v1/forecast?latitude=48.8566&longitude=2.3522&hourly=temperature_2m'
    );
""").fetchdf())
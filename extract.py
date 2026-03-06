import requests
import pandas as pd
from azure.storage.blob import BlobServiceClient
from datetime import datetime
import json
import os
from dotenv import load_dotenv

load_dotenv()

# Configuración
API_KEY = os.getenv("API_FOOTBALL_KEY")
LEAGUE_ID = 262
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER_NAME = "raw"# Temporada con más datos completos

# Azure Storage
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")

def get_fixtures(season):
    """Extrae todos los partidos de la temporada"""
    url = "https://v3.football.api-sports.io/fixtures"
    headers = {"x-apisports-key": API_KEY}
    params = {
        "league": LEAGUE_ID,
        "season": season
    }
    
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    
    partidos = []
    for fixture in data["response"]:
        partidos.append({
            "fixture_id": fixture["fixture"]["id"],
            "date": fixture["fixture"]["date"],
            "home_team": fixture["teams"]["home"]["name"],
            "away_team": fixture["teams"]["away"]["name"],
            "home_goals": fixture["goals"]["home"],
            "away_goals": fixture["goals"]["away"],
            "result": get_result(fixture["goals"]["home"], fixture["goals"]["away"])
        })
    
    return pd.DataFrame(partidos)

def get_result(home_goals, away_goals):
    """Determina el resultado: H=Home, D=Draw, A=Away"""
    if home_goals is None or away_goals is None:
        return None
    if home_goals > away_goals:
        return "H"
    elif home_goals == away_goals:
        return "D"
    else:
        return "A"

def upload_to_azure(df, filename):
    """Sube el DataFrame a Azure Blob Storage"""
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container = blob_service.get_container_client(CONTAINER_NAME)
    
    csv_data = df.to_csv(index=False)
    blob_name = f"liga-mx/{filename}"
    
    container.upload_blob(name=blob_name, data=csv_data, overwrite=True)
    print(f"✅ Subido a Azure: {blob_name}")

if __name__ == "__main__":
    all_fixtures = []
    seasons = [2021, 2022, 2023, 2024]
    
    for season in seasons:
        print(f"Extrayendo temporada {season}...")
        df = get_fixtures(season)
        print(f"  Partidos encontrados: {len(df)}")
        all_fixtures.append(df)
    
    df_total = pd.concat(all_fixtures, ignore_index=True)
    print(f"\nTotal partidos: {len(df_total)}")
    
    filename = f"fixtures_all_{datetime.now().strftime('%Y%m%d')}.csv"
    upload_to_azure(df_total, filename)
import pandas as pd
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from io import StringIO
import os
from dotenv import load_dotenv

load_dotenv()

# Azure Storage
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
RAW_CONTAINER = "raw"
PROCESSED_CONTAINER = "processed"

def download_from_azure(container, blob_name):
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(container)
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    return pd.read_csv(StringIO(data.decode("utf-8")))

def upload_to_azure(df, container, filename):
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(container)
    csv_data = df.to_csv(index=False)
    container_client.upload_blob(name=filename, data=csv_data, overwrite=True)
    print(f"✅ Subido a Azure: {filename}")

def transform(df):
    # Eliminar partidos sin resultado
    df = df.dropna(subset=["home_goals", "away_goals", "result"])

    # Convertir fecha
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Usar solo temporada 2024-2025
    df = df[df["date"].dt.year.isin([2024, 2025])]
    print(f"Partidos temporada 2024-2025: {len(df)}")

    # Convertir resultado a número
    result_map = {"H": 1, "D": 0, "A": -1}
    df["result_num"] = df["result"].map(result_map)

    # Forma reciente local
    df["home_form"] = df.groupby("home_team")["result_num"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean()
    )

    # Forma reciente visitante
    df["away_form"] = df.groupby("away_team")["result_num"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean()
    )

    # Goles promedio últimos 5 partidos
    df["home_goals_avg"] = df.groupby("home_team")["home_goals"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean()
    )

    df["away_goals_avg"] = df.groupby("away_team")["away_goals"].transform(
        lambda x: x.shift(1).rolling(5, min_periods=1).mean()
    )

    # Eliminar filas con NaN
    df = df.dropna(subset=["home_form", "away_form", "home_goals_avg", "away_goals_avg"])

    print(f"Partidos después de transformación: {len(df)}")
    print(df[["home_team", "away_team", "result", "home_form", "away_form"]].head())

    return df

if __name__ == "__main__":
    print("Descargando datos de Azure...")
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(RAW_CONTAINER)
    blobs = list(container_client.list_blobs(name_starts_with="liga-mx/"))
    latest_blob = sorted(blobs, key=lambda x: x.last_modified, reverse=True)[0]

    print(f"Archivo encontrado: {latest_blob.name}")
    df = download_from_azure(RAW_CONTAINER, latest_blob.name)
    print(f"Partidos cargados: {len(df)}")

    print("\nTransformando datos...")
    df_processed = transform(df)

    filename = f"liga-mx/fixtures_processed_{datetime.now().strftime('%Y%m%d')}.csv"
    upload_to_azure(df_processed, PROCESSED_CONTAINER, filename)
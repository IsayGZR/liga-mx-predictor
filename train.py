import pandas as pd
import json
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from io import StringIO
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
import pickle
import os
from dotenv import load_dotenv

load_dotenv()

# Azure Storage
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
PROCESSED_CONTAINER = "processed"
PREDICTIONS_CONTAINER = "predictions"

def download_from_azure(container, blob_name):
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(container)
    blob_client = container_client.get_blob_client(blob_name)
    data = blob_client.download_blob().readall()
    return pd.read_csv(StringIO(data.decode("utf-8")))

def upload_to_azure(data, container, filename):
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(container)
    container_client.upload_blob(name=filename, data=data, overwrite=True)
    print(f"✅ Subido a Azure: {filename}")

if __name__ == "__main__":
    print("Descargando datos procesados de Azure...")
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(PROCESSED_CONTAINER)
    blobs = list(container_client.list_blobs(name_starts_with="liga-mx/"))
    latest_blob = sorted(blobs, key=lambda x: x.last_modified, reverse=True)[0]

    df = download_from_azure(PROCESSED_CONTAINER, latest_blob.name)
    print(f"Partidos cargados: {len(df)}")

    # Features y target
    features = ["home_form", "away_form", "home_goals_avg", "away_goals_avg"]
    X = df[features]
    y = df["result"]

    # Split entrenamiento / prueba
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    # Entrenar modelo Random Forest
    print("\nEntrenando modelo Random Forest...")
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Evaluar
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)
    print(f"\n📊 Accuracy: {accuracy:.2%}")
    print("\nReporte detallado:")
    print(classification_report(y_test, y_pred))

    # Guardar modelo en Azure
    model_binary = pickle.dumps(model)
    model_filename = f"liga-mx/model_{datetime.now().strftime('%Y%m%d')}.pkl"
    upload_to_azure(model_binary, PREDICTIONS_CONTAINER, model_filename)

    # Guardar métricas
    metrics = {
        "accuracy": accuracy,
        "date": datetime.now().strftime("%Y-%m-%d"),
        "total_partidos": len(df),
        "features": features
    }
    upload_to_azure(
        json.dumps(metrics),
        PREDICTIONS_CONTAINER,
        f"liga-mx/metrics_{datetime.now().strftime('%Y%m%d')}.json"
    )
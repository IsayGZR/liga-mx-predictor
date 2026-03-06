import azure.functions as func
import json
import pickle
import pandas as pd
from azure.storage.blob import BlobServiceClient
from io import BytesIO
import os

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

AZURE_CONNECTION_STRING = AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
PREDICTIONS_CONTAINER = "predictions"

def load_model():
    """Descarga el modelo desde Azure Blob Storage"""
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container_client = blob_service.get_container_client(PREDICTIONS_CONTAINER)
    blobs = list(container_client.list_blobs(name_starts_with="liga-mx/model_"))
    latest_blob = sorted(blobs, key=lambda x: x.last_modified, reverse=True)[0]
    blob_client = container_client.get_blob_client(latest_blob.name)
    model_binary = blob_client.download_blob().readall()
    return pickle.loads(model_binary)

def get_team_stats(team, df, is_home=True):
    """Obtiene estadísticas recientes de un equipo"""
    if is_home:
        team_games = df[df["home_team"] == team].tail(5)
        form = team_games["result_num"].mean() if len(team_games) > 0 else 0
        goals_avg = team_games["home_goals"].mean() if len(team_games) > 0 else 0
    else:
        team_games = df[df["away_team"] == team].tail(5)
        form = team_games["result_num"].mean() if len(team_games) > 0 else 0
        goals_avg = team_games["away_goals"].mean() if len(team_games) > 0 else 0
    return form, goals_avg

@app.route(route="predict")
def predict(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        home_team = req_body.get("home_team")
        away_team = req_body.get("away_team")

        if not home_team or not away_team:
            return func.HttpResponse(
                json.dumps({"error": "Se requieren home_team y away_team"}),
                mimetype="application/json",
                status_code=400
            )

        # Cargar modelo
        model = load_model()

        # Cargar datos procesados para obtener estadísticas
        blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
        container_client = blob_service.get_container_client("processed")
        blobs = list(container_client.list_blobs(name_starts_with="liga-mx/"))
        latest_blob = sorted(blobs, key=lambda x: x.last_modified, reverse=True)[0]
        blob_client = container_client.get_blob_client(latest_blob.name)
        data = blob_client.download_blob().readall()
        df = pd.read_csv(BytesIO(data))

        # Obtener estadísticas de los equipos
        home_form, home_goals_avg = get_team_stats(home_team, df, is_home=True)
        away_form, away_goals_avg = get_team_stats(away_team, df, is_home=False)

        # Hacer predicción
        features = [[home_form, away_form, home_goals_avg, away_goals_avg]]
        prediction = model.predict(features)[0]
        probabilities = model.predict_proba(features)[0]
        classes = model.classes_

        prob_dict = {cls: round(float(prob) * 100, 1) for cls, prob in zip(classes, probabilities)}

        result_map = {"H": f"Gana {home_team}", "D": "Empate", "A": f"Gana {away_team}"}

        response = {
            "home_team": home_team,
            "away_team": away_team,
            "prediction": result_map.get(prediction, prediction),
            "probabilities": {
                f"Gana {home_team}": prob_dict.get("H", 0),
                "Empate": prob_dict.get("D", 0),
                f"Gana {away_team}": prob_dict.get("A", 0)
            },
            "stats": {
                "home_form": round(home_form, 2),
                "away_form": round(away_form, 2),
                "home_goals_avg": round(home_goals_avg, 2),
                "away_goals_avg": round(away_goals_avg, 2)
            }
        }

        return func.HttpResponse(
            json.dumps(response),
            mimetype="application/json",
            status_code=200,
            headers={"Access-Control-Allow-Origin": "*"}
        )

    except Exception as e:
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500
        )

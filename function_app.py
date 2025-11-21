import azure.functions as func
import datetime
import json
import logging
import requests
import random
import os
import io
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv


app = func.FunctionApp()
load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

@app.queue_trigger(arg_name="azqueue", 
                   queue_name="requests",
                    connection="QueueAzureWebJobsStorage") 
def QueueTriggerPokeReport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]
    
    logger.info(f"Processing queue message: {id}")
    update_request(id, "inprogress")
    
    logger.info(f"{id} Updated to InProgress")
 
   
    request_info = get_request(id)
    
    logger.info(f"The requested info {request_info}")
    
    pokemons = get_pokemons(request_info[0]["type"],request_info[0]["samplesize"])
    pokemons_detailed = get_pokemon_details(pokemons)
    logger.info(f"got pokemons like {pokemons[0]}")

    
    pokemon_bytes = generate_csv_to_blob(pokemons_detailed)
    logger.info(f"CSV GENERERATED")
    
    blob_name = f"poke_report_{id}.csv"
    upload_csv_to_blob(blob_name, pokemon_bytes)
    
    logger.info(f"Archivo {blob_name} se subio con exito")
    
    url_complete = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"

    update_request(id, "completed", url_complete)


def update_request(id: int, status:str, url:str = None) -> dict:
    payload = {
        "status":status,
        "id": id
    }
    
    if url:
        payload["url"] = url
    
    response = requests.put(f"{DOMAIN}/api/request", json=payload)
    
    return response.json()


def get_request(id:int) -> dict:
    response = requests.get(f"{DOMAIN}/api/request/{id}")
    return response.json()


def get_pokemons(type:str,sample_size: int) -> dict:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"
    logger.info(f"the url is {pokeapi_url}")
    
    response = requests.get(pokeapi_url, timeout=4000)
    data = response.json()
    pokemon_entries = data.get("pokemon", [])

    pokemons = [ p["pokemon"] for p in pokemon_entries]
    if not sample_size:
        return pokemons
    
    sample_size = min(sample_size, len(pokemons))

    return random.sample(pokemons, sample_size)


def generate_csv_to_blob(pokemon_list:list) -> bytes:
    df = pd.DataFrame(pokemon_list)
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()
    return csv_bytes


def upload_csv_to_blob(blob_name:str, csv_data:bytes):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
        
    except Exception as e:
        logger.error(f"Error uploading blob: {e}")
        raise


def get_pokemon_details(pokemons):
    pokemons_details = []
    
    for pokemon in pokemons:
        try:
            response = requests.get(pokemon["url"], timeout=3000)
            data = response.json()
            
            pokemon_data = {
                "name": pokemon["name"],
                "url": pokemon["url"]
            }
            
            for stat in data.get("stats", []):
                stat_name = stat["stat"]["name"]
                pokemon_data[stat_name] = stat["base_stat"]
            
            abilities = data.get("abilities", [])
            abilities_names = [ability["ability"]["name"] for ability in abilities]
            pokemon_data["abilities"] = ", ".join(abilities_names)
            
            pokemons_details.append(pokemon_data)
            
        except Exception as e:
            logger.error(f"Error al obtener detalles de {pokemon['name']}: {e}")
            continue
    
    return pokemons_details
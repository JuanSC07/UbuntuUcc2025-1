import logging
import os
import pickle
import sys
from pathlib import Path
import json

import pika
from fastapi import FastAPI, HTTPException

from pyd_models import PayloadModel

import threading
from pydantic import BaseModel


QUEUE_NAME = os.environ["QUEUENAME"]

# CONFIGURACIÓN PARA LEER JSON
DATA_DIR = Path("/code/app/data")
JSON_FILE = DATA_DIR / "claims.json"

app = FastAPI(title="FastAPI + RabbitMQ + Consumer Demo")
rabbit_params = pika.ConnectionParameters(host="rabbitmq")

@app.on_event("startup")
async def logging_init():
    """Configure logging to output to stdout and format it for easy viewing"""
    ch = logging.StreamHandler(sys.stdout)
    logging.basicConfig(
        level=20,
        format="%(asctime)s [%(levelname)s] %(funcName)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[ch]
    )
    logging.info(f"Starting producer...")

@app.get("/")
def read_root():
    return {"Developer": "Adib Yahaya"}

@app.post("/api")
def accept_payload(payload: PayloadModel):
    payload_dict = payload.dict()
    logging.debug(f"Payload received: {payload_dict}")

    with pika.BlockingConnection(rabbit_params) as connection:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME)
        channel.basic_publish(exchange="", 
                            routing_key=QUEUE_NAME, 
                            body=pickle.dumps(payload_dict))

    return {"status": "received"}

# NUEVOS ENDPOINTS PARA LEER DATOS
@app.get("/claims")
def get_claims():
    """Read and return all claims from JSON file"""
    if not JSON_FILE.exists():
        raise HTTPException(status_code=404, detail="Claims file not found")
    
    try:
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Claims file is corrupted")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading claims: {str(e)}")

@app.get("/claims/{claim_id}")
def get_claim_by_id(claim_id: str):
    """Get a specific claim by ID"""
    if not JSON_FILE.exists():
        raise HTTPException(status_code=404, detail="Claims file not found")
    
    try:
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        
        claims = data.get("claims", [])
        for claim in claims:
            if claim.get("id") == claim_id:
                return claim
        
        raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading claim: {str(e)}")
    


# Modelo para actualización de status
class StatusUpdate(BaseModel):
    status: str

# Lock global para operaciones de archivo
file_lock = threading.Lock()

@app.put("/claims/{claim_id}/status")
def update_claim_status_put(claim_id: str, status_update: StatusUpdate):
    """Update claim status using PUT (complete replacement)"""
    with file_lock:
        if not JSON_FILE.exists():
            raise HTTPException(status_code=404, detail="Claims file not found")
        
        try:
            # Leer datos existentes
            with open(JSON_FILE, "r") as f:
                data = json.load(f)
            
            claims = data.get("claims", [])
            claim_found = False
            
            # Buscar y actualizar el claim
            for i, claim in enumerate(claims):
                if claim.get("id") == claim_id:
                    claims[i]["status"] = status_update.status
                    claims[i]["last_modified"] = "2025-05-23T02:43:47.447767"  # Timestamp actual
                    claim_found = True
                    break
            
            if not claim_found:
                raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found")
            
            # Actualizar metadatos
            data["metadata"]["last_updated"] = "2025-05-23T02:43:47.447767"
            
            # Escribir de vuelta al archivo (operación atómica)
            temp_file = f"{JSON_FILE}.tmp"
            with open(temp_file, "w") as f:
                json.dump(data, f, indent=2)
            
            # Reemplazar archivo original
            os.replace(temp_file, JSON_FILE)
            
            return {
                "message": f"Status updated successfully for claim {claim_id}",
                "id": claim_id,
                "new_status": status_update.status,
                "operation": "PUT"
            }
            
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="Claims file is corrupted")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error updating claim: {str(e)}")

@app.patch("/claims/{claim_id}/status")
def update_claim_status_patch(claim_id: str, status_update: StatusUpdate):
    """Update claim status using PATCH (partial update)"""
    with file_lock:
        if not JSON_FILE.exists():
            raise HTTPException(status_code=404, detail="Claims file not found")
        
        try:
            # Leer datos existentes
            with open(JSON_FILE, "r") as f:
                data = json.load(f)
            
            claims = data.get("claims", [])
            claim_found = False
            old_status = None
            
            # Buscar y actualizar el claim
            for i, claim in enumerate(claims):
                if claim.get("id") == claim_id:
                    old_status = claim.get("status", "Unknown")
                    claims[i]["status"] = status_update.status
                    claims[i]["last_modified"] = "2025-05-23T02:43:47.447767"
                    claim_found = True
                    break
            
            if not claim_found:
                raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found")
            
            # Actualizar metadatos
            data["metadata"]["last_updated"] = "2025-05-23T02:43:47.447767"
            
            # Escribir de vuelta al archivo (operación atómica)
            temp_file = f"{JSON_FILE}.tmp"
            with open(temp_file, "w") as f:
                json.dump(data, f, indent=2)
            
            # Reemplazar archivo original
            os.replace(temp_file, JSON_FILE)
            
            return {
                "message": f"Status updated successfully for claim {claim_id}",
                "id": claim_id,
                "old_status": old_status,
                "new_status": status_update.status,
                "operation": "PATCH"
            }
            
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="Claims file is corrupted")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error updating claim: {str(e)}")

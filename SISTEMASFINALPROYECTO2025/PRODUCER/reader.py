from fastapi import FastAPI, HTTPException, Query
import json
import os
from pathlib import Path
from typing import Optional, List
import threading
from pydantic import BaseModel

app = FastAPI(title="Claims Reader API")

DATA_DIR = Path("/code/app/data")
JSON_FILE = DATA_DIR / "claims.json"

@app.get("/")
def read_root():
    return {"service": "Claims Reader API", "status": "active"}

@app.get("/claims")
def get_all_claims(
    limit: Optional[int] = Query(None, description="Limit number of results"),
    offset: Optional[int] = Query(0, description="Offset for pagination")
):
    """Get all claims with optional pagination"""
    try:
        if not JSON_FILE.exists():
            return {"claims": [], "total": 0, "metadata": {}}
        
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        
        claims = data.get("claims", [])
        total = len(claims)
        
        # Aplicar paginación
        if limit:
            claims = claims[offset:offset + limit]
        else:
            claims = claims[offset:]
        
        return {
            "claims": claims,
            "total": total,
            "metadata": data.get("metadata", {}),
            "pagination": {
                "limit": limit,
                "offset": offset,
                "returned": len(claims)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading claims: {str(e)}")

@app.get("/claims/{claim_id}")
def get_claim_by_id(claim_id: str):
    """Get a specific claim by ID"""
    try:
        if not JSON_FILE.exists():
            raise HTTPException(status_code=404, detail="No claims found")
        
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        
        claims = data.get("claims", [])
        for claim in claims:
            if claim.get("id") == claim_id:
                return claim
        
        raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found")
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Invalid JSON file")

@app.get("/claims/search")
def search_claims(q: str = Query(..., description="Search term")):
    """Search claims by customer name or description"""
    try:
        if not JSON_FILE.exists():
            return {"claims": [], "total": 0}
        
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        
        claims = data.get("claims", [])
        filtered_claims = [
            claim for claim in claims
            if q.lower() in claim.get("customer", "").lower() or
               q.lower() in claim.get("description", "").lower()
        ]
        
        return {"claims": filtered_claims, "total": len(filtered_claims)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

@app.get("/stats")
def get_stats():
    """Get statistics about the claims data"""
    try:
        if not JSON_FILE.exists():
            return {"total_claims": 0, "file_size": 0}
        
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        
        claims = data.get("claims", [])
        file_size = os.path.getsize(JSON_FILE)
        
        # Calcular estadísticas
        total_amount = sum(claim.get("amount", 0) for claim in claims)
        avg_amount = total_amount / len(claims) if claims else 0
        
        return {
            "total_claims": len(claims),
            "total_amount": total_amount,
            "average_amount": avg_amount,
            "file_size_bytes": file_size,
            "metadata": data.get("metadata", {})
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stats error: {str(e)}")





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
            updated_claim = None
            
            # Buscar y actualizar el claim
            for i, claim in enumerate(claims):
                if claim.get("id") == claim_id:
                    claims[i]["status"] = status_update.status
                    claims[i]["last_modified"] = "2025-05-23T02:43:47.447767"
                    updated_claim = claims[i]
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
                "claim": updated_claim,
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
            updated_claim = None
            status_history = []
            
            # Buscar y actualizar el claim
            for i, claim in enumerate(claims):
                if claim.get("id") == claim_id:
                    old_status = claim.get("status", "Unknown")
                    
                    # Mantener historial de cambios de status
                    if "status_history" not in claims[i]:
                        claims[i]["status_history"] = []
                    
                    claims[i]["status_history"].append({
                        "previous_status": old_status,
                        "changed_at": "2025-05-23T02:43:47.447767"
                    })
                    
                    claims[i]["status"] = status_update.status
                    claims[i]["last_modified"] = "2025-05-23T02:43:47.447767"
                    updated_claim = claims[i]
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
                "claim": updated_claim,
                "operation": "PATCH",
                "status_changed": True
            }
            
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="Claims file is corrupted")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error updating claim: {str(e)}")

# Endpoint adicional para ver historial de cambios de status
@app.get("/claims/{claim_id}/status-history")
def get_claim_status_history(claim_id: str):
    """Get status change history for a specific claim"""
    try:
        if not JSON_FILE.exists():
            raise HTTPException(status_code=404, detail="Claims file not found")
        
        with open(JSON_FILE, "r") as f:
            data = json.load(f)
        
        claims = data.get("claims", [])
        for claim in claims:
            if claim.get("id") == claim_id:
                return {
                    "claim_id": claim_id,
                    "current_status": claim.get("status", "Unknown"),
                    "status_history": claim.get("status_history", [])
                }
        
        raise HTTPException(status_code=404, detail=f"Claim {claim_id} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading claim history: {str(e)}")
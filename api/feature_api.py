"""
Feature Store REST API
FastAPI service for feature serving
"""

from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional
import redis
import json
from datetime import datetime
import logging

app = FastAPI(title="Feature Store API", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Redis connection
redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)


class FeatureRequest(BaseModel):
    entity_ids: List[str]
    feature_names: Optional[List[str]] = None


class FeatureRegistration(BaseModel):
    name: str
    description: str
    feature_type: str
    entity: str
    source_table: str
    version: str = "1.0"
    tags: List[str] = []


class FeatureResponse(BaseModel):
    entity_id: str
    features: Dict[str, any]
    timestamp: str


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "redis_connected": redis_client.ping()
    }


@app.post("/features/online", response_model=List[FeatureResponse])
async def get_online_features(request: FeatureRequest):
    """Get features from online store"""
    try:
        logger.info(f"Fetching online features for {len(request.entity_ids)} entities")
        
        pipe = redis_client.pipeline()
        for entity_id in request.entity_ids:
            pipe.get(f"features:{entity_id}")
        
        results = pipe.execute()
        
        responses = []
        for entity_id, result in zip(request.entity_ids, results):
            if result:
                features = json.loads(result)
                
                # Filter by requested feature names if provided
                if request.feature_names:
                    features = {k: v for k, v in features.items() if k in request.feature_names}
                
                responses.append(FeatureResponse(
                    entity_id=entity_id,
                    features=features,
                    timestamp=datetime.now().isoformat()
                ))
            else:
                responses.append(FeatureResponse(
                    entity_id=entity_id,
                    features={},
                    timestamp=datetime.now().isoformat()
                ))
        
        return responses
        
    except Exception as e:
        logger.error(f"Error fetching features: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/features/register")
async def register_feature(feature: FeatureRegistration):
    """Register a new feature"""
    try:
        logger.info(f"Registering feature: {feature.name}")
        
        feature_metadata = {
            'name': feature.name,
            'description': feature.description,
            'feature_type': feature.feature_type,
            'entity': feature.entity,
            'source_table': feature.source_table,
            'version': feature.version,
            'tags': feature.tags,
            'created_at': datetime.now().isoformat()
        }
        
        # Store in Redis
        redis_client.set(
            f"feature_metadata:{feature.name}",
            json.dumps(feature_metadata)
        )
        
        return {
            "status": "success",
            "feature": feature_metadata
        }
        
    except Exception as e:
        logger.error(f"Error registering feature: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/features/list")
async def list_features():
    """List all registered features"""
    try:
        keys = redis_client.keys("feature_metadata:*")
        
        features = []
        for key in keys:
            metadata = json.loads(redis_client.get(key))
            features.append(metadata)
        
        return {
            "count": len(features),
            "features": features
        }
        
    except Exception as e:
        logger.error(f"Error listing features: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/features/{feature_name}/metadata")
async def get_feature_metadata(feature_name: str):
    """Get metadata for a specific feature"""
    try:
        metadata = redis_client.get(f"feature_metadata:{feature_name}")
        
        if not metadata:
            raise HTTPException(status_code=404, detail="Feature not found")
        
        return json.loads(metadata)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching metadata: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/features/{feature_name}/stats")
async def get_feature_stats(feature_name: str):
    """Get statistics for a feature"""
    try:
        # In production, this would query the offline store
        # For now, return mock stats
        return {
            "feature_name": feature_name,
            "count": 10000,
            "mean": 42.5,
            "stddev": 15.2,
            "min": 0,
            "max": 100,
            "null_count": 5,
            "last_updated": datetime.now().isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error fetching stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/features/batch-write")
async def batch_write_features(features: Dict[str, Dict]):
    """Batch write features to online store"""
    try:
        logger.info(f"Batch writing {len(features)} feature sets")
        
        pipe = redis_client.pipeline()
        
        for entity_id, feature_dict in features.items():
            key = f"features:{entity_id}"
            pipe.set(key, json.dumps(feature_dict))
            pipe.expire(key, 86400)  # 24 hour TTL
        
        pipe.execute()
        
        return {
            "status": "success",
            "count": len(features)
        }
        
    except Exception as e:
        logger.error(f"Error batch writing: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

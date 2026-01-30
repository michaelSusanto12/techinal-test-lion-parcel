"""
Image Blur Detection API
FastAPI service for analyzing images
"""
import base64
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import logging

from .blur_detector import is_blur
from .openai_service import describe_image_base64

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Lion Parcel Image API",
    description="Blur detection and image description service",
    version="1.0.0"
)


class ImageRequest(BaseModel):
    image_url: str


class ImageResponse(BaseModel):
    result: str


@app.get("/")
async def root():
    return {"status": "healthy", "service": "Image API"}


@app.post("/analyze", response_model=ImageResponse)
async def analyze_image(request: ImageRequest):
    """
    Check if image is blurry. If not, describe it using OpenAI.
    """
    logger.info(f"Analyzing: {request.image_url}")
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(request.image_url)
            
            if response.status_code != 200:
                raise HTTPException(status_code=400, detail=f"Failed to fetch image: HTTP {response.status_code}")
            
            image_bytes = response.content
        
        content_type = response.headers.get("content-type", "image/jpeg")
        if not content_type.startswith("image/"):
            content_type = "image/jpeg"
        
        blur_detected, blur_score = is_blur(image_bytes)
        logger.info(f"Blur score: {blur_score:.2f}")
        
        if blur_detected:
            return ImageResponse(result="blur")
        
        image_base64 = base64.b64encode(image_bytes).decode('utf-8')
        description = await describe_image_base64(image_base64, content_type)
        logger.info(f"Description: {description[:100]}...")
        
        return ImageResponse(result=description)
        
    except httpx.RequestError as e:
        logger.error(f"Network error: {e}")
        raise HTTPException(status_code=500, detail=f"Network error: {str(e)}")
    except ValueError as e:
        logger.error(f"Processing error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health_check():
    return {"status": "healthy"}

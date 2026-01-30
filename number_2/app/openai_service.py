"""
OpenAI Vision API integration
"""
import os
from openai import AsyncOpenAI
import logging

logger = logging.getLogger(__name__)

client = AsyncOpenAI(
    api_key=os.getenv("OPENAI_API_KEY"),
    base_url=os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1")
)

MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")


async def describe_image(image_url: str) -> str:
    """Get image description from GPT-4 Vision"""
    try:
        logger.info(f"Calling API with model: {MODEL}, base_url: {client.base_url}")
        
        response = await client.chat.completions.create(
            model=MODEL,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Describe this image in one paragraph. Be concise but descriptive."
                        },
                        {
                            "type": "image_url",
                            "image_url": {"url": image_url, "detail": "low"}
                        }
                    ]
                }
            ],
            max_tokens=300
        )
        
        logger.info(f"API Response: {response}")
        
        if response and response.choices and len(response.choices) > 0:
            choice = response.choices[0]
            if choice.message and choice.message.content:
                return choice.message.content.strip()
        
        return "Unable to describe image"
        
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        raise ValueError(f"Failed to describe image: {str(e)}")


async def describe_image_base64(image_base64: str, media_type: str = "image/jpeg") -> str:
    """Describe image using base64 encoded data"""
    try:
        response = await client.chat.completions.create(
            model=MODEL,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Describe this image in one paragraph."
                        },
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{media_type};base64,{image_base64}",
                                "detail": "low"
                            }
                        }
                    ]
                }
            ],
            max_tokens=300
        )
        
        if response and response.choices and len(response.choices) > 0:
            choice = response.choices[0]
            if choice.message and choice.message.content:
                return choice.message.content.strip()
        
        return "Unable to describe image"
        
    except Exception as e:
        logger.error(f"OpenAI API error: {e}")
        raise ValueError(f"Failed to describe image: {str(e)}")

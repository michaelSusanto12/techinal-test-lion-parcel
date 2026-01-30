"""
Summarize image analysis results to CSV
"""
import csv
import os
import httpx
import asyncio
from pathlib import Path

API_URL = os.getenv("API_URL", "http://localhost:8000")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lionparcel")
IMAGE_FOLDER = os.getenv("IMAGE_FOLDER", "/app/image_dataset")
OUTPUT_FILE = os.getenv("OUTPUT_FILE", "/app/output/summary.csv")


async def analyze_image(client: httpx.AsyncClient, image_url: str) -> str:
    """Send image to API for analysis"""
    try:
        response = await client.post(
            f"{API_URL}/analyze",
            json={"image_url": image_url},
            timeout=120.0
        )
        
        if response.status_code == 200:
            return response.json()["result"]
        else:
            return f"error: HTTP {response.status_code}"
            
    except Exception as e:
        return f"error: {str(e)}"


async def process_all_images():
    """Process all images and save results to CSV"""
    Path(OUTPUT_FILE).parent.mkdir(parents=True, exist_ok=True)
    
    valid_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}
    image_files = [f for f in os.listdir(IMAGE_FOLDER) 
                   if Path(f).suffix.lower() in valid_extensions]
    
    if not image_files:
        print(f"No images found in {IMAGE_FOLDER}")
        return
    
    print(f"Processing {len(image_files)} images...")
    
    results = []
    
    async with httpx.AsyncClient() as client:
        for i, filename in enumerate(image_files, 1):
            image_url = f"http://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{filename}"
            print(f"[{i}/{len(image_files)}] {filename}")
            print(f"  URL: {image_url}")
            
            result = await analyze_image(client, image_url)
            results.append({
                'filename': filename,
                'image_url': image_url,
                'result': result
            })
            
            preview = result[:50] + "..." if len(result) > 50 else result
            print(f"  -> {preview}")
    
    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['filename', 'image_url', 'result'])
        writer.writeheader()
        writer.writerows(results)
    
    blur_count = sum(1 for r in results if r['result'] == 'blur')
    error_count = sum(1 for r in results if r['result'].startswith('error'))
    described_count = len(results) - blur_count - error_count
    
    print(f"\nDone! Saved to {OUTPUT_FILE}")
    print(f"Total: {len(results)}, Blur: {blur_count}, Described: {described_count}, Errors: {error_count}")


if __name__ == "__main__":
    asyncio.run(process_all_images())

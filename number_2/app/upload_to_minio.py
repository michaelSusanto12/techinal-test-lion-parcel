"""
Upload images to MinIO bucket
"""
import os
from pathlib import Path
from minio import Minio
from minio.error import S3Error

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "lionparcel")
IMAGE_FOLDER = os.getenv("IMAGE_FOLDER", "/app/image_dataset")


def get_content_type(filename: str) -> str:
    ext = Path(filename).suffix.lower()
    types = {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.gif': 'image/gif',
        '.webp': 'image/webp',
        '.bmp': 'image/bmp',
    }
    return types.get(ext, 'application/octet-stream')


def upload_images():
    """Upload all images to MinIO"""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        print(f"Created bucket: {MINIO_BUCKET}")
    
    valid_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp'}
    
    uploaded = 0
    for filename in os.listdir(IMAGE_FOLDER):
        if Path(filename).suffix.lower() in valid_extensions:
            filepath = os.path.join(IMAGE_FOLDER, filename)
            content_type = get_content_type(filename)
            
            try:
                client.fput_object(MINIO_BUCKET, filename, filepath, content_type=content_type)
                print(f"Uploaded: {filename}")
                uploaded += 1
            except S3Error as e:
                print(f"Error uploading {filename}: {e}")
    
    print(f"\nDone! {uploaded} files uploaded")
    print(f"Access at: http://{MINIO_ENDPOINT}/{MINIO_BUCKET}/")


def list_images():
    """List all images in bucket"""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    
    print(f"\nFiles in {MINIO_BUCKET}:")
    for obj in client.list_objects(MINIO_BUCKET):
        print(f"  {obj.object_name} ({obj.size} bytes)")


if __name__ == "__main__":
    print("Uploading images to MinIO...")
    upload_images()
    list_images()

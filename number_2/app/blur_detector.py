"""
Blur detection using Laplacian variance method
"""
import cv2
import numpy as np
from typing import Tuple


def is_blur(image_bytes: bytes, threshold: float = 500.0) -> Tuple[bool, float]:
    """
    Detect if image is blurry using edge detection.
    Lower variance = more blur.
    Returns (is_blurry, variance_score)
    """
    nparr = np.frombuffer(image_bytes, np.uint8)
    image = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
    
    if image is None:
        raise ValueError("Cannot decode image")
    
    max_dim = 500
    height, width = image.shape[:2]
    if max(height, width) > max_dim:
        scale = max_dim / max(height, width)
        image = cv2.resize(image, None, fx=scale, fy=scale)
    
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    laplacian = cv2.Laplacian(gray, cv2.CV_64F)
    variance = laplacian.var()
    
    return variance < threshold, variance


def get_blur_level(variance: float) -> str:
    """Categorize blur level based on variance"""
    if variance < 50:
        return "very_blurry"
    elif variance < 100:
        return "blurry"
    elif variance < 200:
        return "slightly_blurry"
    elif variance < 500:
        return "sharp"
    else:
        return "very_sharp"

import json
import uuid
from urllib.parse import urlparse


def generate_error_message():
    return {
        "type": "https://tools.ietf.org/html/rfc7231#section-6.5.1",
        "title": "Validation failed",
        "status": 400,
        "traceId": uuid.uuid4().hex,  # уникальный traceId
        "errors": {
            "Email": ["Invalid"]
        }
    }

import base64
import hmac
import hashlib


def calculate_secret_hash(email, client_id, key):
    key = bytes(key, 'utf-8')
    message = bytes(email + client_id, 'utf-8')
    return base64.b64encode(hmac.new(key, message, digestmod=hashlib.sha256).digest()).decode()

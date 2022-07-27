import hashlib


def hash512(text: str) -> str:
    text = str(hashlib.sha512(text.encode('utf-8')).hexdigest())
    return text

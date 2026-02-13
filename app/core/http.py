import requests

session = requests.Session()

def get(url: str, params: dict | None = None, timeout: int = 20):
    r = session.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def post(url: str, json_body: dict, timeout: int = 20):
    r = session.post(url, json=json_body, timeout=timeout)
    r.raise_for_status()
    return r.json()

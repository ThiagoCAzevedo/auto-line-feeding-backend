from dotenv import load_dotenv
import os
import requests

load_dotenv("config/.env")


class AssemblyLineApi:
    def __init__(self):
        self.al_url = os.getenv("AL_API_ENDPOINT")

    def get_raw_response(self):
        response = requests.get(self.al_url, verify=False, timeout=10)
        response.raise_for_status()
        return response.json()
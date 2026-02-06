from dotenv import load_dotenv
import os, requests, urllib3

load_dotenv("config/.env")
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AccessAssemblyLineApi:
    def __init__(self):
        self.al_url = os.getenv("AL_API_ENDPOINT")

    def get_json_response(self):
        response = requests.get(self.al_url, verify=False, timeout=5)
        response.raise_for_status()
        return response.json()
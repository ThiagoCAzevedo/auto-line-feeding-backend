from dotenv import load_dotenv
import os, requests
load_dotenv()


class AssemblyLineApi:
    def __init__(self):
        self.al_url = os.getenv("AL_API_ENDPOINT")

    def _return_response(self):
        return requests.get(self.al_url, verify=False)
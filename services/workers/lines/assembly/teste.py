import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url = "https://10.135.121.158:40520/v1/getPositions"

try:
    response = requests.get(url, verify=False, timeout=10)
    print("Status code:", response.status_code)
    print("Resposta:")
    print(response.text)
except Exception as e:
    print("Erro ao chamar o endpoint:", e)
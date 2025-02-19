import requests

url = "https://staging.lizeedu.com.br/api/v2/students"
headers = {
    "Authorization": "Token f138893cee459fc57c263a02a6ac4451f991554f",
    "accept": "application/json"
}

response = requests.get(url, headers=headers)

print("Status Code:", response.status_code)
print("Response Headers:", response.headers)
print("Response Body:", response.text)

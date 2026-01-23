import requests

url = 'https://sgaa.cemaden.gov.br/SGAA/rest/controle-token/tokens'

headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}

payload = {
    "email": "bruno36399@gmail.com",
    "password": "Lol30106497"
}
def get_token():
    try:

        response = requests.post(url, headers=headers, json=payload)


        if response.status_code == 200:
            print("Sucesso! Token recebido:")
            #print(response.json()) 
        else:
            print(f"Erro: {response.status_code}")
            print(response.text)

    except requests.exceptions.RequestException as e:
        print(f"Erro na conexão: {e}")

    dados = response.json()

    return dados['token']


import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

URL_TOKEN = 'https://sgaa.cemaden.gov.br/SGAA/rest/controle-token/tokens'

HEADERS = {
    'accept': 'application/json',
    'Content-Type': 'application/json'
}

# Adicione suas contas aqui
CONTAS = [
    {"email": "bruno36399@gmail.com", "password": "Lol30106497"},
    {"email": "bruno@gmail.com", "password": "Lol30106497"},
    {"email": "bruno1@gmail.com", "password": "Lol30106497"},
    {"email": "bruno2@gmail.com", "password": "Lol30106497"},
    {"email": "bruno3@gmail.com", "password": "Lol30106497"},
    {"email": "bruno4@gmail.com", "password": "Lol30106497"},
    {"email": "bruno5@gmail.com", "password": "Lol30106497"},
    {"email": "bruno6@gmail.com", "password": "Lol30106497"},
    {"email": "bruno7@gmail.com", "password": "Lol30106497"},
    {"email": "bruno8@gmail.com", "password": "Lol30106497"},
    {"email": "bruno9@gmail.com", "password": "Lol30106497"},
    {"email": "bruno10@gmail.com", "password": "Lol30106497"},
    {"email": "bruno11@gmail.com", "password": "Lol30106497"},
    {"email": "bruno12@gmail.com", "password": "Lol30106497"},
    {"email": "bruno13@gmail.com", "password": "Lol30106497"},
    {"email": "bruno14@gmail.com", "password": "Lol30106497"},
    {"email": "bruno15@gmail.com", "password": "Lol30106497"},
]

def autenticar_conta(conta):
    """
    Função auxiliar isolada para autenticar uma única conta.
    Retorna o token se tiver sucesso, ou None em caso de falha.
    """
    try:
        # Adicionado um timeout de 10 segundos por segurança para evitar que a thread fique travada
        response = requests.post(URL_TOKEN, headers=HEADERS, json=conta, timeout=10)
        
        if response.status_code == 200:
            dados = response.json()
            token = dados.get('token')
            if token:
                print(f"V [OK] Token gerado para {conta['email']}")
                return token
        else:
            print(f"X [ERRO] Falha para {conta['email']}: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"X [ERRO] Conexão falhou para {conta['email']}: {e}")
        
    return None

def get_tokens():
    """
    Autentica em várias contas em paralelo e retorna uma lista de tokens válidos.
    """
    tokens_validos = []
    
    print(f"--- Buscando tokens para {len(CONTAS)} contas em paralelo ---")
    
    # max_workers=10 significa que ele processará até 10 contas simultaneamente
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Envia todas as requisições de forma simultânea
        futuros = [executor.submit(autenticar_conta, conta) for conta in CONTAS]
        
        # as_completed permite capturar o resultado de cada requisição assim que ela finalizar
        for futuro in as_completed(futuros):
            resultado_token = futuro.result()
            if resultado_token:
                tokens_validos.append(resultado_token)

    if not tokens_validos:
        print("!!! NENHUM TOKEN FOI GERADO !!!")
        
    return tokens_validos
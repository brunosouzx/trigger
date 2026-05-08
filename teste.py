import requests
import json

def api_para_json(url_da_api, nome_do_arquivo):
    """
    Busca dados de uma API e os salva em um arquivo .json.
    """
    print(f"Buscando dados de: {url_da_api}...")
    
    try:
        # 1. Faz a requisição GET para a API
        resposta = requests.get(url_da_api)
        
        # 2. Verifica se a requisição falhou (ex: erro 404, 500)
        resposta.raise_for_status()
        
        # 3. Converte a resposta da API para um formato que o Python entende (Dicionário/Lista)
        dados = resposta.json()
        
        # 4. Cria e abre um arquivo em modo de escrita ('w')
        with open(nome_do_arquivo, 'w', encoding='utf-8') as arquivo:
            # Salva os dados no arquivo. 
            # indent=4 deixa o arquivo formatado e fácil de ler (pretty print)
            json.dump(dados, arquivo, ensure_ascii=False, indent=4)
            
        print(f"✅ Sucesso! Dados salvos no arquivo: {nome_do_arquivo}")
        
    except requests.exceptions.RequestException as erro_api:
        print(f"❌ Erro de conexão com a API: {erro_api}")
    except ValueError:
        print("❌ Erro: A API não retornou um formato JSON válido.")
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado: {e}")

# ==========================================
# Exemplo de Uso
# ==========================================
if __name__ == "__main__":
    # URL de uma API pública falsa para testar
    url_teste = "http://dados.apac.pe.gov.br:41120/cemaden/" 
    
    # Nome do arquivo que será criado na mesma pasta do script
    arquivo_saida = "apac.json"
    
    # Chama a função
    api_para_json(url_teste, arquivo_saida)
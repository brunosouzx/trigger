import requests
import json

def api_para_json_filtrado(url_da_api, nome_do_arquivo):
    """
    Busca dados de uma API, filtra pela estação [APAC] e salva em .json.
    """
    print(f"Buscando dados de: {url_da_api}...")
    
    try:
        # 1. Faz a requisição para a API
        resposta = requests.get(url_da_api)
        resposta.raise_for_status()
        
        # 2. Converte a resposta para um formato Python
        dados = resposta.json()
        
        # 3. FILTRAGEM DOS DADOS
        # Assumindo que a API retorna uma lista de dicionários (o mais comum)
        if isinstance(dados, list):
            # Cria uma nova lista apenas com os itens que têm "[APAC]" na "Estação"
            dados_filtrados = [
                item for item in dados 
                # Pega o valor de "Estação" (se não existir, usa texto vazio)
                # e verifica se "[APAC]" faz parte desse texto
                if "[APAC]" in str(item.get("Estação", ""))
            ]
            print(f"🔍 Foram encontrados {len(dados_filtrados)} registros da [APAC].")
        else:
            # Caso a API retorne um formato diferente de uma lista direta
            print("⚠️ Aviso: A API não retornou uma lista direta. Salvando os dados sem filtrar.")
            dados_filtrados = dados
        
        # 4. Salva APENAS os dados filtrados no arquivo .json
        with open(nome_do_arquivo, 'w', encoding='utf-8') as arquivo:
            json.dump(dados_filtrados, arquivo, ensure_ascii=False, indent=4)
            
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
    # Substitua pela URL real da sua API
    url_sua_api ="http://dados.apac.pe.gov.br:41120/cemaden/"  
    
    arquivo_saida = "dados_apac.json"
    
    # Chama a função
    api_para_json_filtrado(url_sua_api, arquivo_saida)
import requests
import psycopg2
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv() 
DB_CONFIG = os.getenv('DATABASE_URL')

# Substitua pela URL real dessa NOVA API
URL_NOVA_API = "http://dados.apac.pe.gov.br:41120/cemaden/" 

def comparar_nova_api_sem_duplicatas():
    if not DB_CONFIG:
        raise ValueError("A variável DATABASE_URL não foi encontrada. Verifique seu arquivo .env")

    conn = None
    try:
        # 1. Conectar ao banco e buscar as estações do Cemaden
        print("Conectando ao banco de dados...")
        conn = psycopg2.connect(DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT codestacao FROM cemadem_estacao WHERE codestacao IS NOT NULL;")
        codigos_cemaden = {linha[0] for linha in cursor.fetchall()}
        print(f"Total de estações na tabela 'cemadem_estacao': {len(codigos_cemaden)}")

        # 2. Buscar dados da API
        print("Buscando dados da API...")
        response = requests.get(URL_NOVA_API)
        response.raise_for_status()
        dados_json = response.json() 
        print(f"Total de registros brutos lidos na API: {len(dados_json)}")

        # ---------------------------------------------------------
        # 3. NOVIDADE: Filtro para remover duplicatas da própria API
        # ---------------------------------------------------------
        estacoes_api_unicas = {}
        
        for item in dados_json:
            codigo_api = item.get('Codigo_gmmc')
            
            # Só adiciona se tiver um código válido e se AINDA NÃO estiver no nosso dicionário
            if codigo_api and codigo_api not in estacoes_api_unicas:
                estacoes_api_unicas[codigo_api] = item
                
        print(f"Total de estações ÚNICAS na API após filtro: {len(estacoes_api_unicas)}\n")

        # 4. Fazer o comparativo (agora usando a lista limpa)
        estacoes_em_comum = []

        # .values() pega apenas os dados das estações, ignorando a chave do dicionário
        for item in estacoes_api_unicas.values():
            codigo_api = item.get('Codigo_gmmc')
            nome_estacao = item.get('Estação')

            # Verifica se o código único da API existe no 'set' do Cemaden
            if codigo_api in codigos_cemaden:
                estacoes_em_comum.append({
                    'codigo': codigo_api,
                    'nome': nome_estacao
                })

        # 5. Exibir o Relatório Final
        print("-" * 50)
        print("📊 RELATÓRIO DE COMPARAÇÃO (DADOS LIMPOS)")
        print("-" * 50)
        print(f"Estações registradas no banco (Cemaden): {len(codigos_cemaden)}")
        print(f"Estações únicas detectadas na API: {len(estacoes_api_unicas)}")
        print(f"✅ Estações IGUAIS encontradas em ambas as fontes: {len(estacoes_em_comum)}\n")

        if estacoes_em_comum:
            print("Lista de estações em comum:")
            for estacao in estacoes_em_comum:
                print(f" -> Código: {estacao['codigo']} | Nome na API: {estacao['nome']}")

    except requests.exceptions.RequestException as e:
        print(f"Erro de comunicação com a API: {e}")
    except psycopg2.Error as e:
        print(f"Erro de banco de dados PostgreSQL: {e}")
    except ValueError as e:
        print(f"Erro ao processar dados da API: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("\nConexão com o banco fechada.")

if __name__ == "__main__":
    comparar_nova_api_sem_duplicatas()
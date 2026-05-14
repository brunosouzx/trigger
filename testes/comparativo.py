import requests
import psycopg2
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv() 
DB_CONFIG = os.getenv('DATABASE_URL')

# Substitua pela URL real da API
URL_API = "https://geoportal.apac.pe.gov.br/server/rest/services/met_monitoramento_chuvas_pe/MapServer/4/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=horas_24%20desc&resultOffset=0&resultRecordCount=2000" 

def comparar_estacoes_cemaden_apac():
    if not DB_CONFIG:
        raise ValueError("A variável DATABASE_URL não foi encontrada. Verifique seu arquivo .env")

    conn = None
    try:
        # 1. Conectar ao banco e buscar as estações do Cemaden
        print("Conectando ao banco de dados...")
        conn = psycopg2.connect(DB_CONFIG)
        cursor = conn.cursor()

        # Busca apenas os códigos das estações no Cemaden
        cursor.execute("SELECT codestacao FROM cemadem_estacao WHERE codestacao IS NOT NULL;")
        
        # Armazena em um 'set' para busca ultra-rápida O(1)
        codigos_cemaden = {linha[0] for linha in cursor.fetchall()}
        print(f"Total de estações na tabela 'cemadem_estacao': {len(codigos_cemaden)}")

        # 2. Buscar dados da API
        print("Buscando dados da API...")
        response = requests.get(URL_API)
        response.raise_for_status()
        dados_json = response.json()
        
        features = dados_json.get('features', [])
        print(f"Total de estações na API: {len(features)}\n")

        # 3. Fazer o comparativo
        estacoes_em_comum = []

        for feature in features:
            atributos = feature.get('attributes', {})
            codigo_api = atributos.get('codigo_gmmc')
            nome_estacao = atributos.get('nome')

            # Verifica se o código da API existe no 'set' do Cemaden
            if codigo_api and codigo_api in codigos_cemaden:
                estacoes_em_comum.append({
                    'codigo': codigo_api,
                    'nome': nome_estacao
                })

        # 4. Exibir o Relatório Final
        print("-" * 50)
        print("📊 RELATÓRIO DE COMPARAÇÃO DE ESTAÇÕES")
        print("-" * 50)
        print(f"Estações registradas no Cemaden: {len(codigos_cemaden)}")
        print(f"Estações detectadas na API: {len(features)}")
        print(f"✅ Estações IGUAIS encontradas em ambas as fontes: {len(estacoes_em_comum)}\n")

        # 5. Listar detalhadamente as duplicidades
        if estacoes_em_comum:
            print("Lista de estações em comum (pelo código GMMC/Codestacao):")
            for estacao in estacoes_em_comum:
                print(f" -> Código: {estacao['codigo']} | Nome na API: {estacao['nome']}")

    except requests.exceptions.RequestException as e:
        print(f"Erro de comunicação com a API: {e}")
    except psycopg2.Error as e:
        print(f"Erro de banco de dados PostgreSQL: {e}")
    finally:
        # Garante que a conexão seja fechada mesmo se houver erro
        if conn:
            cursor.close()
            conn.close()
            print("\nConexão com o banco fechada.")

if __name__ == "__main__":
    comparar_estacoes_cemaden_apac()
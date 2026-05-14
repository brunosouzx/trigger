import requests
import psycopg2
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv() 
DB_CONFIG = os.getenv('DATABASE_URL')

# Substitua pelas URLs reais das suas APIs
URL_API_1 = "https://geoportal.apac.pe.gov.br/server/rest/services/met_monitoramento_chuvas_pe/MapServer/4/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=horas_24%20desc&resultOffset=0&resultRecordCount=2000" # Aquela que tem a estrutura "features"
URL_API_2 = "http://dados.apac.pe.gov.br:41120/cemaden/" # Aquela que é uma lista direta

def listar_apenas_estacoes_novas():
    if not DB_CONFIG:
        raise ValueError("A variável DATABASE_URL não foi encontrada. Verifique seu arquivo .env")

    conn = None
    try:
        # 1. Conectar ao banco e buscar as estações que JÁ EXISTEM
        print("Conectando ao banco de dados...")
        conn = psycopg2.connect(DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute("SELECT codestacao FROM cemadem_estacao WHERE codestacao IS NOT NULL;")
        codigos_cemaden = {linha[0] for linha in cursor.fetchall()}
        print(f"Total de estações já registradas no Cemaden: {len(codigos_cemaden)}\n")

        # 2. Dicionário UNIFICADO para juntar as duas APIs sem duplicatas
        estacoes_api_unicas = {}

        # ---------------------------------------------------------
        # 3. Processar API 1 (Estrutura 'features')
        # ---------------------------------------------------------
        print("Buscando dados da API 1...")
        resp1 = requests.get(URL_API_1)
        resp1.raise_for_status()
        
        for feature in resp1.json().get('features', []):
            atributos = feature.get('attributes', {})
            codigo = atributos.get('codigo_gmmc')
            nome = atributos.get('nome')
            
            if codigo and codigo not in estacoes_api_unicas:
                estacoes_api_unicas[codigo] = {'nome': nome, 'fonte': 'API 1'}

        # ---------------------------------------------------------
        # 4. Processar API 2 (Lista Direta)
        # ---------------------------------------------------------
        print("Buscando dados da API 2...")
        resp2 = requests.get(URL_API_2)
        resp2.raise_for_status()
        
        for item in resp2.json():
            codigo = item.get('Codigo_gmmc')
            nome = item.get('Estação')
            
            # Só adiciona se não existir na API 2 E não tiver vindo da API 1
            if codigo and codigo not in estacoes_api_unicas:
                estacoes_api_unicas[codigo] = {'nome': nome, 'fonte': 'API 2'}

        print(f"Total de estações ÚNICAS somando as duas APIs: {len(estacoes_api_unicas)}\n")

        # ---------------------------------------------------------
        # 5. Filtrar apenas as "NÃO DUPLICADAS" com o banco (As Novas)
        # ---------------------------------------------------------
        estacoes_novas = []
        
        for codigo, dados in estacoes_api_unicas.items():
            if codigo not in codigos_cemaden:
                estacoes_novas.append({
                    'codigo': codigo,
                    'nome': dados['nome'],
                    'fonte': dados['fonte']
                })

        # 6. Exibir o Relatório Final
        print("-" * 60)
        print("📊 RELATÓRIO DE ESTAÇÕES NOVAS (NÃO DUPLICADAS NO BANCO)")
        print("-" * 60)
        print(f"Estações que precisam ser inseridas: {len(estacoes_novas)}\n")

        if estacoes_novas:
            print("Lista de estações exclusivas para inserção:")
            for estacao in estacoes_novas:
                print(f" -> [{estacao['fonte']}] Código: {estacao['codigo']} | Nome: {estacao['nome']}")
        else:
            print("Nenhuma estação nova encontrada. O banco já possui todas as estações das APIs.")

    except requests.exceptions.RequestException as e:
        print(f"Erro de comunicação com alguma das APIs: {e}")
    except psycopg2.Error as e:
        print(f"Erro de banco de dados PostgreSQL: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("\nConexão com o banco fechada.")

if __name__ == "__main__":
    listar_apenas_estacoes_novas()
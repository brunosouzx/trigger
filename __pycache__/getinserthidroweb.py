import requests
import psycopg2
from dotenv import load_dotenv
import os

# 1. Configurações do Banco de Dados (Substitua com seus dados)
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")
DB_HOST = "localhost"
DB_NAME = "seu_banco_de_dados"
DB_USER = "seu_usuario"
DB_PASS = "sua_senha"

def importar_estacoes():
    # Conectando ao banco de dados
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()
    except Exception as e:
        print(f"Erro ao conectar no banco: {e}")
        return

    # 2. URL da primeira API (Modifiquei o 'where' para trazer apenas PERNAMBUCO direto da API para ficar mais rápido)
    url_lista = "https://portal1.snirh.gov.br/server/rest/services/SGH/CotasReferencia2/MapServer/2/query?f=json&resultOffset=0&resultRecordCount=15000&where=Estado%3D'PERNAMBUCO'&orderByFields=&outFields=*&returnGeometry=false"
    
    print("Buscando estações de Pernambuco na API...")
    response_lista = requests.get(url_lista)
    dados_lista = response_lista.json()
    
    features = dados_lista.get('features', [])
    print(f"Encontradas {len(features)} estações. Iniciando processamento...")

    for feature in features:
        attr = feature['attributes']
        
        estcodigo = attr.get('estcodigo')
        nome_estacao = attr.get('Nome')
        responsavel = attr.get('Responsavel')
        municipio = attr.get('Municipio')
        
        # 3. Buscar o ID da cidade no banco de dados
        fk_id_cidade = None
        if municipio:
            # Usando UPPER para garantir que a busca ignore letras maiúsculas/minúsculas
            cursor.execute("SELECT id FROM public.cidades WHERE UPPER(nome_cidade) = UPPER(%s)", (municipio,))
            resultado_cidade = cursor.fetchone()
            if resultado_cidade:
                fk_id_cidade = resultado_cidade[0]
            else:
                print(f"Aviso: Cidade '{municipio}' não encontrada na tabela cidades para a estação {nome_estacao}.")

        # 4. Chamar a segunda API para pegar a Latitude e Longitude (Geometry)
        url_geo = f"https://portal1.snirh.gov.br/server/rest/services/SGH/CotasReferencia2/MapServer/2/query?f=json&objectIds={estcodigo}&outFields=*&returnGeometry=true&spatialRel=esriSpatialRelIntersects"
        
        response_geo = requests.get(url_geo)
        dados_geo = response_geo.json()
        
        longitude = None
        latitude = None
        
        # Extraindo X (Longitude) e Y (Latitude)
        geo_features = dados_geo.get('features', [])
        if geo_features and 'geometry' in geo_features[0]:
            geometry = geo_features[0]['geometry']
            longitude = geometry.get('x')
            latitude = geometry.get('y')

        # 5. Inserir na tabela hidro_estacao
        try:
            sql_insert = """
                INSERT INTO public.hidro_estacao 
                (fk_id_cidade, nome_estacao, latitude, longitude, ativo, codestacao, responsavel)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (codestacao) DO NOTHING;
            """
            # Passando True para a coluna 'ativo' e convertendo o estcodigo para string
            cursor.execute(sql_insert, (fk_id_cidade, nome_estacao, latitude, longitude, True, str(estcodigo), responsavel))
            print(f"Estação {nome_estacao} processada com sucesso.")
            
        except Exception as e:
            print(f"Erro ao inserir estação {nome_estacao}: {e}")
            conn.rollback() # Desfaz a transação em caso de erro

    # Confirma as alterações no banco e fecha a conexão
    conn.commit()
    cursor.close()
    conn.close()
    print("Processo finalizado!")

if __name__ == "__main__":
    importar_estacoes()
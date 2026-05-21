import requests
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv() 
DB_CONFIG = os.getenv('DATABASE_URL')

if not DB_CONFIG:
    raise ValueError("A variável DATABASE_URL não foi encontrada. Verifique seu arquivo .env")

URL_API = "https://geoportal.apac.pe.gov.br/server/rest/services/met_monitoramento_chuvas_pe/MapServer/4/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=horas_24%20desc&resultOffset=0&resultRecordCount=2000" 

# Transformei a sua lista em um "set" para o Python buscar de forma ultra-rápida
CODIGOS_ALVO = {
    '260900601A', '260230802A', '261630802A', '260775201A', '261450101A', '260550901A',
    '261160623A', '260580601A', '260810701A', '260100301A', '261340401A', '260780201A',
    '260060901A', '261060802A', '261220801A', '260850301A', '260765301A', '261380001A',
    '261170501A', '260170601A', '260490801A', '261010301A', '260370201A', '260845301A',
    '260670501A', '261470902A', '261485702A', '261530001A', '260820602A', '260090601A',
    '260360301A', '260230801A', '261210901A', '260005402A', '261090501A', '261380002A',
    '260120101A', '260105201A', '260460101A', '260030201A', '261540901A', '260350401A',
    '260220901A', '261520102A', '261190301A', '261300801C', '260415501A', '261550801A',
    '261310701A', '261250501A', '260890901A', '260540001A', '261520101A', '260760401A',
    '261020201A', '260070801A', '261160615A', '261500301A', '260210001A', '260610102A',
    '261050901A', '260440301A', '261080601A', '261160620A', '261245501A', '260620003A',
    '260270401A', '261180402A', '261330502A', '260800801A'
}

def inserir_lista_especifica_apac():
    try:
        print("Buscando dados da API...")
        response = requests.get(URL_API)
        response.raise_for_status()
        features = response.json().get('features', [])
        
        print("Conectando ao banco de dados...")
        conn = psycopg2.connect(DB_CONFIG) 
        cursor = conn.cursor()

        # Verifica o que já existe na apac_estacao para não dar erro de duplicidade no INSERT
        cursor.execute("SELECT codestacao FROM apac_estacao WHERE codestacao IS NOT NULL;")
        estacoes_ja_na_apac = {linha[0] for linha in cursor.fetchall()}

        query_insert = """
            INSERT INTO apac_estacao (fk_id_cidade, nome_estacao, latitude, longitude, ativo, codestacao)
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        contador_insercoes = 0
        estacoes_ignoradas = 0

        print("\nIniciando processo de inserção...")
        
        for feature in features:
            atributos = feature.get('attributes', {})
            codigo_api = atributos.get('codigo_gmmc')

            # 1. Verifica se o código da API está na sua lista de 70 estações
            if codigo_api in CODIGOS_ALVO:
                
                # 2. Verifica se JÁ FOI inserida na apac_estacao antes
                if codigo_api in estacoes_ja_na_apac:
                    estacoes_ignoradas += 1
                    continue

                # 3. Prepara os dados para o INSERT
                try:
                    fk_id_cidade = int(atributos.get('cod_municipio'))
                except (ValueError, TypeError):
                    fk_id_cidade = None

                nome_estacao = atributos.get('nome')
                latitude = atributos.get('latitude')
                longitude = atributos.get('longitude')
                
                # 4. Executa a inserção garantindo que entra como ATIVA (True)
                cursor.execute(query_insert, (
                    fk_id_cidade, 
                    nome_estacao, 
                    latitude, 
                    longitude, 
                    True, 
                    codigo_api
                ))
                
                contador_insercoes += 1
                print(f" [+] Inserida: {codigo_api} - {nome_estacao}")

        # Efetiva as mudanças no banco
        conn.commit()
        
        print("-" * 50)
        print("✅ INSERÇÃO CONCLUÍDA COM SUCESSO")
        print("-" * 50)
        print(f"Estações da sua lista cadastradas na apac_estacao: {contador_insercoes}")
        if estacoes_ignoradas > 0:
            print(f"⚠️ Estações ignoradas por já existirem na tabela apac_estacao: {estacoes_ignoradas}")

    except requests.exceptions.RequestException as e:
        print(f"Erro de comunicação com a API: {e}")
    except psycopg2.Error as e:
        print(f"Erro de banco de dados PostgreSQL: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
    finally:
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()
            print("\nConexão com o banco fechada.")

if __name__ == "__main__":
    inserir_lista_especifica_apac()
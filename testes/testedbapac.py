import json
import psycopg2
import requests
import os
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Carrega as variáveis de ambiente (como o DATABASE_URL do Render/Local)
load_dotenv()
DB_URL = "postgresql://admin:sRNKBig6uHVyWqEnMuBuBXMG6lS1uL2d@dpg-d5q4816r433s73fpqakg-a.virginia-postgres.render.com/ihalagou_db_5kcd"

URL_API_APAC = "http://dados.apac.pe.gov.br:41120/cemaden/" # Substitua pelo link real

def cadastrar_estacoes():
    print("=== INICIANDO CADASTRO DE ESTAÇÕES APAC ===")
    
    # 1. Obter dados da API (usando o mock fornecido para teste)
    #Na versão final, descomente as linhas abaixo:
    response = requests.get(URL_API_APAC, timeout=30)
    response.raise_for_status()
    dados_json = response.json()
    '''
    # Mock com a estrutura que você enviou:
    dados_json = [
        {
            "Estação": "[APAC] Prefeitura",
            "Data-hora": "2026-05-07 04:00:00",
            "Codigo_gmmc": "261100201A",
            "Dados_completos": "{\"codestacao\":\"261100201A\",\"latitude\":-8.9801,\"longitude\":-38.21851,\"cidade\":\"PETROL\\u00c2NDIA\",\"nome\":\"Petrol\\u00e2ndia [Prefeitura] - APAC\",\"tipo\":\"Pluviom\\u00e9trica\",\"uf\":\"PE\",\"chuva\":0,\"dataHora\":\"2026-05-07 07:00:00.0\"}"
        },
        {
            "Estação": "[APAC] Prefeitura Municipal",
            "Data-hora": "2026-05-07 04:00:00",
            "Codigo_gmmc": "261200001A",
            "Dados_completos": "{\"codestacao\":\"261200001A\",\"latitude\":-8.32887,\"longitude\":-35.70838,\"cidade\":\"SAIR\\u00c9\",\"nome\":\"Prefeitura\",\"tipo\":\"Pluviom\\u00e9trica\",\"uf\":\"PE\",\"chuva\":0,\"dataHora\":\"2026-05-07 07:00:00.0\"}"
        },
        {
            "Estação": "Estação Sem APAC", # Exemplo que será ignorado
            "Data-hora": "2026-05-07 04:00:00",
            "Codigo_gmmc": "000000000A",
            "Dados_completos": "{\"codestacao\":\"000000000A\",\"latitude\":-7.8300,\"longitude\":-35.8841}"
        },
        {
            "Estação": "[APAC] Sem Coordenadas", # Exemplo que será ignorado
            "Data-hora": "2026-05-07 04:00:00",
            "Codigo_gmmc": "111111111A",
            "Dados_completos": "{\"codestacao\":\"111111111A\",\"latitude\":null,\"longitude\":null}"
        }
    ]
    '''

    dados_para_inserir = []

   # 2. Processar e filtrar os dados
    for item in dados_json:
        nome_original = item.get("Estação", "")
        
        # Filtra apenas se tiver [APAC]
        if "[APAC]" in nome_original:
            dados_completos_str = item.get("Dados_completos", "{}")
            
            try:
                dados_completos_dict = json.loads(dados_completos_str)
                
                latitude = dados_completos_dict.get("latitude")
                longitude = dados_completos_dict.get("longitude")
                codestacao = dados_completos_dict.get("codestacao")
                nome_estacao = dados_completos_dict.get("nome", nome_original)
                
                # Verifica se as coordenadas e o código existem
                if latitude is not None and longitude is not None and codestacao:
                    
                    # EXTRAÇÃO DO ID DA CIDADE (Os 7 primeiros caracteres)
                    fk_id_cidade = None
                    if len(codestacao) >= 7:
                        try:
                            # Pega do índice 0 até o 6 e converte para Inteiro
                            fk_id_cidade = int(codestacao[:7])
                        except ValueError:
                            # Se falhar a conversão (ex: tem letras onde deviam ser números), mantém nulo
                            fk_id_cidade = None
                    
                    tupla_estacao = (
                        fk_id_cidade,   # Agora recebe os 7 primeiros números
                        nome_estacao,   # nome_estacao
                        latitude,       # latitude
                        longitude,      # longitude
                        True,           # ativo
                        codestacao      # codestacao
                    )
                    dados_para_inserir.append(tupla_estacao)
                    
            except json.JSONDecodeError:
                print(f"Erro ao ler JSON da estação {nome_original}")

    # 3. Inserir no Banco de Dados
    if not dados_para_inserir:
        print("Nenhuma estação nova/válida encontrada para inserir.")
        return

    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        query = """
            INSERT INTO public.apac_estacao 
            (fk_id_cidade, nome_estacao, latitude, longitude, ativo, codestacao)
            VALUES %s
            ON CONFLICT (codestacao) DO NOTHING
        """
        
        execute_values(cursor, query, dados_para_inserir)
        conn.commit()
        
        print(f"Sucesso! {len(dados_para_inserir)} estações processadas para o banco.")

    except Exception as e:
        print(f"Erro no banco de dados: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    cadastrar_estacoes()
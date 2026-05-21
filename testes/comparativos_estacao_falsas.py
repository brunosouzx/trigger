import requests
import psycopg2
import os
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv() 
DB_CONFIG = os.getenv('DATABASE_URL')

# Substitua pela URL real da API
URL_API = "https://geoportal.apac.pe.gov.br/server/rest/services/met_monitoramento_chuvas_pe/MapServer/4/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=horas_24%20desc&resultOffset=0&resultRecordCount=2000" 

def filtrar_e_comparar_estacoes():
    if not DB_CONFIG:
        raise ValueError("A variável DATABASE_URL não foi encontrada. Verifique seu arquivo .env")

    conn = None
    try:
        # 1. Conectar ao banco e buscar as estações inativas (ativo = false)
        conn = psycopg2.connect(DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute("SELECT codestacao FROM cemadem_estacao WHERE codestacao IS NOT NULL AND ativo = false;")
        
        # Set com os códigos do banco para o comparativo
        codigos_inativos_bd = {linha[0] for linha in cursor.fetchall()}

        # 2. Buscar dados da API
        response = requests.get(URL_API)
        response.raise_for_status()
        features = response.json().get('features', [])
        
        estacoes_que_batem = []

        # 3. Filtrar a API e fazer o comparativo
        for feature in features:
            atributos = feature.get('attributes', {})
            codigo_api = atributos.get('codigo_gmmc')
            nome_estacao = atributos.get('nome')
            
            # Lembre-se de verificar o nome exato desse campo no JSON
            acumulado_1h = atributos.get('chuva_1h') 

            if acumulado_1h is not None and codigo_api:
                try:
                    valor_acumulado = float(acumulado_1h)
                    
                    # PASSO A: Filtra a API (acumulado >= 0)
                    if valor_acumulado >= 0:
                        
                        # PASSO B: Faz o comparativo (o código da API está na lista de inativos do BD?)
                        if codigo_api in codigos_inativos_bd:
                            estacoes_que_batem.append({
                                'codigo': codigo_api,
                                'nome': nome_estacao,
                                'acumulado_1h': valor_acumulado
                            })
                except ValueError:
                    continue

        # 4. Mostrar na tela as estações que batem com o comparativo
        print("-" * 75)
        print("🔍 RESULTADO DO COMPARATIVO: ESTAÇÕES INATIVAS NO BD COM DADOS NA API")
        print("-" * 75)
        
        if estacoes_que_batem:
            print(f"Foram encontradas {len(estacoes_que_batem)} estações que batem com os critérios:\n")
            
            # Opcional: ordenar pelo volume de chuva
            estacoes_que_batem.sort(key=lambda x: x['acumulado_1h'], reverse=True)
            
            for estacao in estacoes_que_batem:
                print(f" -> Código: {estacao['codigo']} | Nome: {estacao['nome']} | Acumulado 1h: {estacao['acumulado_1h']}mm")
        else:
            print("Nenhuma estação inativa no banco reportou chuva >= 0 na API no momento.")

    except requests.exceptions.RequestException as e:
        print(f"Erro de comunicação com a API: {e}")
    except psycopg2.Error as e:
        print(f"Erro de banco de dados PostgreSQL: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    filtrar_e_comparar_estacoes()
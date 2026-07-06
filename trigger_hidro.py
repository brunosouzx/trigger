import psycopg2
import requests
import os
from datetime import datetime, timezone, timedelta
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

# Fuso horário do Brasil (UTC-3)
FUSO_BR = timezone(timedelta(hours=-3))

# URL da API SNIRH
URL_API_SNIRH = "https://portal1.snirh.gov.br/server/rest/services/SGH/CotasReferencia2/MapServer/2/query?f=json&resultOffset=0&resultRecordCount=15000&where=1%3D1&orderByFields=&outFields=*&returnGeometry=false&spatialRel=esriSpatialRelIntersects"

def sincronizar_dados_hidroweb():
    print("\n=== INICIANDO SINCRONIZAÇÃO DE MEDIÇÕES SNIRH ===")
    
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        # 1. Busca os códigos das estações cadastradas na tabela hidro_estacao
        # Convertendo para string direto na consulta para garantir a comparação correta
        cursor.execute("SELECT codestacao::varchar FROM public.hidro_estacao")
        estacoes_cadastradas = {linha[0] for linha in cursor.fetchall()}
        
        if not estacoes_cadastradas:
            print("Nenhuma estação encontrada na tabela hidro_estacao. Abortando.")
            return

        # 2. Faz a requisição à API
        print("Buscando dados da API SNIRH...")
        response = requests.get(URL_API_SNIRH, timeout=30)
        response.raise_for_status()
        dados_json = response.json()
        
        dados_para_inserir = []

        # 3. Acessa a lista "features" do JSON
        features = dados_json.get("features", [])

        # 4. Processar e filtrar os dados
        for item in features:
            attr = item.get("attributes", {})
            
            # Pega o estcodigo e converte para string para comparar e inserir no VARCHAR(20)
            fk_codestacao = str(attr.get("estcodigo"))
            nome_estacao = attr.get("Nome", "Desconhecida")
            
            # FILTRO: Ignora se a estação não estiver na tabela hidro_estacao
            if fk_codestacao not in estacoes_cadastradas:
                continue 
            
            valor = attr.get("Ult_Dado")
            timestamp_ms = attr.get("Data_ult_dado")
            
            if timestamp_ms is not None and valor is not None:
                try:
                    # A API manda o timestamp em milissegundos, precisamos dividir por 1000
                    # Definimos como UTC primeiro e depois convertemos para o Fuso BR
                    data_utc = datetime.fromtimestamp(timestamp_ms / 1000.0, tz=timezone.utc)
                    data_br = data_utc.astimezone(FUSO_BR)
                    
                    tupla_medicao = (
                        fk_codestacao,
                        valor,
                        data_br
                    )
                    dados_para_inserir.append(tupla_medicao)
                    
                except Exception as e:
                    print(f"Erro ao processar data da estação {nome_estacao} (Cód: {fk_codestacao}): {e}")

        # 5. Inserir na Base de Dados
        if not dados_para_inserir:
            print("Nenhum dado novo de estação cadastrada foi encontrado para inserir.")
            return

        print(f"Preparando para inserir {len(dados_para_inserir)} medições...")

        # Query de inserção com tratamento de conflito baseado na chave única criada anteriormente
        query = """
            INSERT INTO public.hidro_medicao 
            (fk_codestacao, valor, data)
            VALUES %s
            ON CONFLICT (fk_codestacao, data) DO NOTHING
        """
        
        execute_values(cursor, query, dados_para_inserir)
        conn.commit()
        
        print(f"✅ Sucesso! {len(dados_para_inserir)} registros lidos e processados pelo banco.")

    except Exception as e:
        print(f"❌ Erro geral no script: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    sincronizar_dados_hidroweb()
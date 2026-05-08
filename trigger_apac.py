import json
import psycopg2
import requests
import os
from datetime import datetime
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

URL_API_APAC = "http://dados.apac.pe.gov.br:41120/cemaden/" # Substitua pelo link real

def sincronizar_medicoes_apac():
    print("\n=== INICIANDO SINCRONIZAÇÃO DE MEDIÇÕES APAC ===")
    
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        # Busca os códigos de todas as estações cadastradas
        cursor.execute("SELECT codestacao FROM public.apac_estacao")
        estacoes_cadastradas = {linha[0] for linha in cursor.fetchall()}
        
        # 1. Fazer requisição à API
        response = requests.get(URL_API_APAC, timeout=30)
        response.raise_for_status()
        dados_json = response.json()
        
        # MOCK DE DADOS PARA TESTE
        #dados_json = [
        #    {
        #        "Estação": "[APAC] Estação Teste",
        #        "Data-hora": "2026-05-07 04:24:32",
        #        "Codigo_gmmc": "260030201A",
        #        "Dados_completos": "{\"chuva\":\"0.2\"}"
        #    },
        #    {
        #        "Estação": "[APAC] Prefeitura",
        #        "Data-hora": "2026-05-07 04:00:00",
        #        "Codigo_gmmc": "261100201A", 
        #        "Dados_completos": "{\"chuva\":1.5}"
        #    }
        #]

        dados_para_inserir = []

        # 2. Processar e filtrar os dados
        for item in dados_json:
            nome_estacao = item.get("Estação", "")
            
            if "[APAC]" in nome_estacao:
                data_hora_str = item.get("Data-hora")
                fk_codestacao = item.get("Codigo_gmmc")
                dados_completos_str = item.get("Dados_completos", "{}")
                
                # Verifica se a estação existe na base de dados
                if fk_codestacao not in estacoes_cadastradas:
                    print(f"⚠️ Aviso: Medição ignorada. A estação {fk_codestacao} ({nome_estacao}) não está na tabela apac_estacao.")
                    continue 
                
                try:
                    dados_completos_dict = json.loads(dados_completos_str)
                    chuva = dados_completos_dict.get("chuva")
                    
                    if chuva is not None and data_hora_str:
                        valor_chuva = float(chuva)
                        
                        # Converte a string diretamente para data (sem aplicar fuso horário)
                        data_obj = datetime.strptime(data_hora_str, "%Y-%m-%d %H:%M:%S")
                        
                        tupla_medicao = (
                            fk_codestacao,
                            'pluviometria',
                            valor_chuva,
                            data_obj, # Insere a data tal como veio da API
                            True,
                            False
                        )
                        dados_para_inserir.append(tupla_medicao)
                        
                except Exception as e:
                    print(f"Erro ao processar medição da estação {nome_estacao}: {e}")

        # 3. Inserir na Base de Dados
        if not dados_para_inserir:
            print("Nenhuma medição válida encontrada para inserir.")
            return

        query = """
            INSERT INTO public.apac_medicao 
            (fk_codestacao, tipo_medicao, valor, data, is_pluviometro, is_geotecnico)
            VALUES %s
            ON CONFLICT (fk_codestacao, data, tipo_medicao) DO NOTHING
        """
        
        execute_values(cursor, query, dados_para_inserir)
        conn.commit()
        
        print(f"✅ Sucesso! {len(dados_para_inserir)} medições sincronizadas com a base de dados.")

    except Exception as e:
        print(f"❌ Erro na base de dados: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    sincronizar_medicoes_apac()
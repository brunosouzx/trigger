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

# Substitua pela URL da nova API
URL_API_APAC_ACUMULADOS = "https://geoportal.apac.pe.gov.br/server/rest/services/met_monitoramento_chuvas_pe/MapServer/4/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=horas_24%20desc&resultOffset=0&resultRecordCount=2000" 

def sincronizar_acumulados_apac():
    print("\n=== INICIANDO SINCRONIZAÇÃO DE ACUMULADOS ===")
    
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        # 1. Busca os códigos de todas as estações cadastradas na base
        cursor.execute("SELECT codestacao FROM public.apac_estacao")
        estacoes_cadastradas = {linha[0] for linha in cursor.fetchall()}
        
        # 2. Fazer requisição à nova API
        response = requests.get(URL_API_APAC_ACUMULADOS, timeout=30)
        response.raise_for_status()
        dados_json = response.json()
        
        dados_para_inserir = []

        # 3. Acessa a lista "features" do novo JSON
        features = dados_json.get("features", [])

        # 4. Processar e filtrar os dados
        for item in features:
            # Todos os dados úteis estão dentro de "attributes"
            attr = item.get("attributes", {})
            
            fk_codestacao = attr.get("codigo_gmmc")
            nome_estacao = attr.get("nome", "Desconhecida")
            
            # FILTRO PRINCIPAL: Ignora se a estação não estiver na tabela apac_estacao
            if fk_codestacao not in estacoes_cadastradas:
                continue 
            
            data_hora_bruta = attr.get("ultima_leitura_data_hora")
            
            if data_hora_bruta:
                try:
                    # A API manda "2026-05-13 00:30:00 00:30:00"
                    # Fatiamos [:19] para pegar apenas "2026-05-13 00:30:00"
                    data_hora_limpa = data_hora_bruta[:19]
                    data_obj = datetime.strptime(data_hora_limpa, "%Y-%m-%d %H:%M:%S")
                    
                    tupla_acumulados = (
                        fk_codestacao,
                        attr.get("hora_1"),    # acc_1h
                        attr.get("horas_3"),   # acc_3h
                        attr.get("horas_6"),   # acc_6h
                        attr.get("horas_12"),  # acc_12h
                        attr.get("horas_24"),  # acc_24h
                        attr.get("horas_48"),  # acc_48h
                        attr.get("horas_72"),  # acc_72h
                        data_obj               # data_hora
                    )
                    dados_para_inserir.append(tupla_acumulados)
                    
                except Exception as e:
                    print(f"Erro ao processar datas da estação {nome_estacao}: {e}")

        # 5. Inserir na Base de Dados
        if not dados_para_inserir:
            print("Nenhum acumulado de estação cadastrada foi encontrado para inserir.")
            return

        # Query atualizada com tratamento de conflito
        query = """
            INSERT INTO public.apac_acumulados 
            (fk_codestacao, acc_1h, acc_3h, acc_6h, acc_12h, acc_24h, acc_48h, acc_72h, data_hora)
            VALUES %s
            ON CONFLICT (fk_codestacao, data_hora) DO NOTHING
        """
        
        execute_values(cursor, query, dados_para_inserir)
        conn.commit()
        
        print(f"✅ Sucesso! {len(dados_para_inserir)} registros de acumulados sincronizados.")

    except Exception as e:
        print(f"❌ Erro na base de dados: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    sincronizar_acumulados_apac()
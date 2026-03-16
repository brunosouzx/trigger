import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import pytz
import os
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor

load_dotenv()
db_url = os.getenv("DATABASE_URL")
TABLE_DESTINO = 'medicao_iha'

MAPA_API_KEYS = {
    3228255: "VNTV6D3PJDIUWTUI", 
    3212148: "KHWVXJ78F5FUBXEU", 
    2998477: "47FQKQ61NWJTRLWS",  
    3215410: "N56C6F6T7697DBF2" , 
    3222304: "AZRC6XU0DMPNANK7"  
}

def processar_unico_totem(totem):
    id_iha, nome_totem = totem
    eh_pluviometro = "PLUVI" in nome_totem.upper()
    eh_pep_pluviometro = "PEP" in nome_totem.upper()
    api_key = MAPA_API_KEYS.get(id_iha)
    
    if not api_key:
        print(f"⚠️ AVISO: Totem '{nome_totem}' (ID {id_iha}) sem API Key. Pulando...")
        return []

    url = f"https://api.thingspeak.com/channels/{id_iha}/feeds.json?api_key={api_key}&results=12"
    dados_extraidos = []
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 404:
            return []

        data = response.json()
        feeds = data.get('feeds', [])

        for feed in feeds:
            try:
                data_str = feed.get('created_at')
                if not data_str: continue
                dt_utc = datetime.strptime(data_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.utc)
                fuso_brasil = pytz.timezone('America/Sao_Paulo')
                data_hora_brasil = dt_utc.astimezone(fuso_brasil)
            except (ValueError, TypeError): continue 

            if eh_pep_pluviometro:
                if feed.get('field3'):
                    try: dados_extraidos.append((id_iha, 'pluviometro', round(float(feed['field3']) * 0.2, 2), data_hora_brasil))
                    except ValueError: pass
            elif eh_pluviometro:
                if feed.get('field2'):
                    try:
                        basculadas = float(feed['field2'])
                        if basculadas > 0:
                            basculadas = max(0.0, basculadas - 2.0)
                        dados_extraidos.append((id_iha, 'pluviometro', round(basculadas * 0.2, 2), data_hora_brasil))
                    except ValueError: pass
            else:
                if feed.get('field2'):
                    try: dados_extraidos.append((id_iha, 'metros', float(feed['field5']), data_hora_brasil))
                    except (ValueError, TypeError): pass

                campo_bateria = 'field2' if eh_pep_pluviometro else 'field3'
                if feed.get(campo_bateria):
                    # O script manda 'bateria', mas o DB vai ignorar nas buscas graças à nova arquitetura!
                    try: dados_extraidos.append((id_iha, 'bateria', float(feed[campo_bateria]), data_hora_brasil))
                    except (ValueError, TypeError): pass

        return dados_extraidos
    except Exception as e:
        print(f"Erro ao processar {nome_totem}: {e}")
        return []

def sincronizar_totens():
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        cursor.execute("SELECT id, nome FROM iha_totem WHERE ativo = TRUE")
        totens = cursor.fetchall()

        if not totens: return

        todos_dados_para_inserir = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            resultados = executor.map(processar_unico_totem, totens)
            for resultado in resultados:
                todos_dados_para_inserir.extend(resultado)

        if todos_dados_para_inserir:
            query = f"""
                INSERT INTO {TABLE_DESTINO} (fk_id_iha, tipo_medicao, valor, data_hora)
                VALUES %s
                ON CONFLICT (fk_id_iha, tipo_medicao, data_hora) DO NOTHING
            """
            execute_values(cursor, query, todos_dados_para_inserir)
            conn.commit()
            print(f" -> Sucesso! {len(todos_dados_para_inserir)} registros inseridos no IHA.")

    except psycopg2.Error as e:
        print(f"Erro geral de Banco no IHA: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
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

# --- CONFIGURAÇÃO DE CHANNELS E REGRAS ---
MAPA_API_KEYS = {
    3228255: "VNTV6D3PJDIUWTUI", 
    3212148: "KHWVXJ78F5FUBXEU", 
    3222304: "AZRC6XU0DMPNANK7",
    2998477: "47FQKQ61NWJTRLWS", # Olinda Caixa d'água
    2998478: "DEE7HH658UTUCFY8", # Jardim Atlântico
    3215410: "N56C6F6T7697DBF2", # Peixinhos
    3215407: "RAOVQF5WGZYL6Q6W", # Tabajara
    3215409: "27J9LC1KY002URPX", # Sapucaia
    3215411: "WFS2OKPOGELHBFON", # GAC
    3215438: "0MHLC8OZJAVBO28W", # Monte
    3236623: "YF2YQMR2ZJNG61H0", # Recife - Campina do Barreto
    3236624: "7IIZ55QY6Y44HGI3", # Rio Morno
    3236625: "8CSHIR09NI27R0MT", # Sapo Nu
    3236626: "13MVTDKVUALHOQRV", # Coripos
    3236622: "RUAYNA7D695IYTDM", # Jardim Sao Paulo
    3222367: "LITPGF7XE4KQN436", # Desafios - Olinda
    3222368: "G4GWVTCQXFP2DYF6", # Desafios - Paulista
    3222371: "PSF02M46K19VIXHP", # Desafios - Igarassu
    3222370: "7M6C3OWKLP2MZUXL", # Desafios - Itapissuma
    3222373: "AEBM9WPHAQ1F81OS", # Desafios - Camaragibe (E São Lourenço)
    3222305: "NI1YEOI1HKOWAIDV", # Pluviometro Camaragibe 1
    3222315: "TVQLECYG30EACWQH", # Pluviometro Camaragibe 2
    3222318: "T0597F200WR6LHN7", # Pluviometro Sao Lourenço 1
    3222317: "7OU79BTK1C24KCVV"  # Pluviometro Sao Lourenço 2
}

# --- LISTAS DE REGRAS DE NEGÓCIO ---
IDS_RECIFE_BATERIA = [3236623, 3236624, 3236625, 3236626, 3236622]

ID_OLINDA_HS = 2998477
IDS_HS_COM_DEFEITO = [] # Sapucaia removido daqui

# Sensores que devem forçar o valor 0.0 na leitura
IDS_FORCAR_ZERO_NIVEL = [2998477, 3215407, 3215409] # Caixa d'agua, Tabajara, Sapucaia
IDS_FORCAR_ZERO_PLUVI = [3222305, 3222315, 3222318, 3222317,2998478,3215410,3215411, 3215438] # Camaragibe 1 e 2, Sao Lourenço 1 e 2

def processar_unico_totem(totem):
    id_iha, nome_totem = totem
    eh_pluviometro = "PLUVI" in nome_totem.upper()
    eh_pep_pluviometro = "PEP" in nome_totem.upper()
    
    # --- REDIRECIONAMENTO DE CANAL ---
    canal_thingspeak = id_iha
    # Se o ID for o fictício de São Lourenço, puxa os dados do canal de Camaragibe
    if id_iha == 3212373: 
        canal_thingspeak = 3222373

    api_key = MAPA_API_KEYS.get(canal_thingspeak)
    
    if not api_key:
        print(f"⚠️ AVISO: Totem '{nome_totem}' (ID {id_iha}) sem API Key. Pulando...")
        return []

    # Faz a requisição usando o canal_thingspeak
    url = f"https://api.thingspeak.com/channels/{canal_thingspeak}/feeds.json?api_key={api_key}&results=12"
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

            # --- PLUVIÓMETROS ---
            if eh_pep_pluviometro:
                if id_iha in IDS_FORCAR_ZERO_PLUVI:
                    dados_extraidos.append((id_iha, 'pluviometro', 0.0, data_hora_brasil))
                elif feed.get('field3'):
                    try: 
                        valor = float(feed['field3']) * 0.2
                        valor = max(0.0, valor)
                        dados_extraidos.append((id_iha, 'pluviometro', round(valor, 2), data_hora_brasil))
                    except ValueError: pass
                    
            elif eh_pluviometro:
                if id_iha in IDS_FORCAR_ZERO_PLUVI:
                    dados_extraidos.append((id_iha, 'pluviometro', 0.0, data_hora_brasil))
                elif feed.get('field2'):
                    try:
                        basculadas = float(feed['field2'])
                        if basculadas > 0:
                            basculadas = max(0.0, basculadas - 2.0)
                        
                        valor = basculadas * 0.2
                        valor = max(0.0, valor)
                        dados_extraidos.append((id_iha, 'pluviometro', round(valor, 2), data_hora_brasil))
                    except ValueError: pass
            
            # --- ECOPOSTES E NÍVEL ---
            else:
                # Nível (Metros)
                if id_iha in IDS_FORCAR_ZERO_NIVEL:
                    # Força diretamente o 0.0 sem fazer cálculos
                    dados_extraidos.append((id_iha, 'metros', 0.0, data_hora_brasil))
                elif id_iha == ID_OLINDA_HS:
                    if feed.get('field4'):
                        try:
                            hs_raw = float(feed['field4'])
                            hs_corrigido = ((hs_raw - 6400) / 25600) * 5
                            hs_corrigido = max(0.0, hs_corrigido)
                            dados_extraidos.append((id_iha, 'metros', round(hs_corrigido, 3), data_hora_brasil))
                        except (ValueError, TypeError): pass
                elif id_iha in IDS_HS_COM_DEFEITO:
                    pass # Ignora completamente a leitura de metros
                else:
                    if feed.get('field5'):
                        try: 
                            valor_hs = float(feed['field5'])
                            valor_hs = max(0.0, valor_hs)
                            dados_extraidos.append((id_iha, 'metros', round(valor_hs, 3), data_hora_brasil))
                        except (ValueError, TypeError): pass

                # Bateria (O processamento da bateria continua igual, não é forçado a zero)
                campo_bateria = 'field2' if eh_pep_pluviometro else 'field3'
                if feed.get('field2') and not eh_pep_pluviometro and not eh_pluviometro:
                    campo_bateria = 'field2'
                    
                if feed.get(campo_bateria):
                    try: 
                        valor_bateria = float(feed[campo_bateria])
                        
                        if id_iha in IDS_RECIFE_BATERIA:
                            valor_bateria = valor_bateria * 1.1373
                            
                        dados_extraidos.append((id_iha, 'bateria', round(valor_bateria, 2), data_hora_brasil))
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
            print(f" -> Sucesso! {len(todos_dados_para_inserir)} registos inseridos no IHA.")

    except psycopg2.Error as e:
        print(f"Erro geral de Banco no IHA: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()
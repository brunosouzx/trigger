import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import pytz

# --- CONFIGURAÇÕES DO BANCO ---
db_url="postgresql://ihalagou_mq4l_user:1MK5i26pWskzRbjJZD4VK68JmHv3BGqH@dpg-d5pc847pm1nc73btfl50-a.virginia-postgres.render.com/ihalagou_mq4l"

TABLE_DESTINO = 'medicao_iha' 

# --- MAPEAMENTO DE IDs PARA API KEYS ---
MAPA_API_KEYS = {
    3228255: "VNTV6D3PJDIUWTUI", # IHA 1
    3212148: "KHWVXJ78F5FUBXEU", # IHA 2
    2998477: "47FQKQ61NWJTRLWS",  # IHA 3
    3215410: "N56C6F6T7697DBF2"  # IHA 4
}

def sincronizar_totens():
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        print("Buscando totens cadastrados no banco...")
        # Busca ID e Nome para identificar quem é PLUVI
        cursor.execute("SELECT id, nome FROM iha_totem WHERE ativo = TRUE")
        totens = cursor.fetchall()

        if not totens:
            print("Nenhum totem ativo encontrado no banco de dados.")
            return

        for totem in totens:
            id_iha = totem[0]
            nome_totem = totem[1]
            
            # Verifica se é pluviômetro pelo nome (Ex: "Totem Pluvi 01")
            eh_pluviometro = "PLUVI" in nome_totem.upper()

            api_key = MAPA_API_KEYS.get(id_iha)
            
            if not api_key:
                print(f"⚠️ AVISO: Totem '{nome_totem}' (ID {id_iha}) sem API Key. Pulando...")
                continue

            url = f"https://api.thingspeak.com/channels/{id_iha}/feeds.json?api_key={api_key}&results=12"
            
            # Exibe no log qual lógic{id_iha}) [{tipo_logica}] ---")
            
            try:
                response = requests.get(url)
                if response.status_code == 404:
                    print(f"Erro 404: Canal {id_iha} não encontrado.")
                    continue

                data = response.json()
                feeds = data.get('feeds', [])

                dados_para_inserir = []

                for feed in feeds:
                    # --- 1. DATA (created_at) ---
                    try:
                        data_str = feed.get('created_at')
                        if not data_str: continue

                        dt_utc = datetime.strptime(data_str, "%Y-%m-%dT%H:%M:%SZ")
                        dt_utc = dt_utc.replace(tzinfo=pytz.utc)
                        fuso_brasil = pytz.timezone('America/Sao_Paulo')
                        data_hora_brasil = dt_utc.astimezone(fuso_brasil)
                    except (ValueError, TypeError):
                        continue 

                    # --- 2. VALOR (LÓGICA ESPECÍFICA) ---
                    if eh_pluviometro:
                        # === LÓGICA DE PLUVIÔMETRO (FIELD 2) ===
                        if feed.get('field2'):
                            try:
                                basculadas = float(feed['field2'])
                                
                                # Regra 1: Subtrai 1 do contador bruto (se maior que 0)
                                if basculadas > 0:
                                    basculadas = basculadas - 1
                                
                                # Regra 2: Converte basculadas em mm
                                # Cada basculada = 0.2 mm de chuva
                                milimetros = basculadas * 0.2
                                
                                # Arredonda para 2 casas decimais para ficar bonito no banco (Ex: 0.40)
                                milimetros = round(milimetros, 2)
                                
                                dados_para_inserir.append((id_iha, 'pluviometro', milimetros, data_hora_brasil))
                            except ValueError:
                                pass
                    else:
                        # === LÓGICA DE NÍVEL DE RIO (FIELD 5 via FIELD 2) ===
                        if feed.get('field2'): 
                            try:
                                metros = float(feed['field5'])
                                dados_para_inserir.append((id_iha, 'metros', metros, data_hora_brasil))
                            except (ValueError, TypeError):
                                pass 

                    # --- 3. BATERIA (FIELD 3) - Comum a todos ---
                    if feed.get('field3'): 
                        try:
                            bateria = float(feed['field3'])
                            dados_para_inserir.append((id_iha, 'bateria', bateria, data_hora_brasil))
                        except (ValueError, TypeError):
                            pass 

                # --- 4. INSERÇÃO NO BANCO ---
                if dados_para_inserir:
                    query = f"""
                        INSERT INTO {TABLE_DESTINO} (fk_id_iha, tipo_medicao, valor, data_hora)
                        VALUES %s
                        ON CONFLICT (fk_id_iha, tipo_medicao, data_hora) DO NOTHING
                    """
                    execute_values(cursor, query, dados_para_inserir)
                    conn.commit()
                    print(f" -> Sucesso! {len(dados_para_inserir)} registros inseridos.")
                else:
                    print(f" -> Nenhum dado novo.")

            except Exception as e_totem:
                print(f"Erro ao processar {nome_totem}: {e_totem}")
                conn.rollback()

    except psycopg2.Error as e:
        print(f"Erro geral de Banco: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    sincronizar_totens()
import asyncio
import aiohttp
import psycopg2
from psycopg2.extras import execute_values
import os
import pytz
from dateutil import parser
from dotenv import load_dotenv

# --- CONFIGURAÇÃO BANCO DE DADOS ---
load_dotenv()
db_url = os.getenv("DATABASE_URL")
TABLE_DESTINO = 'medicao_iha'

# --- CONFIGURAÇÃO ADAFRUIT ---
AIO_KEY = "aio_aKHj49Tf2cjLmMAbbrvze61l9Mrx"
USERNAME = "IHA"
GRUPO_URL = "https://io.adafruit.com/api/v2/IHA/groups/iha0006"

# ID que você definiu no INSERT da tabela iha_totem
ID_TOTEM_BANCO = 1210365 

async def fetch_json(session, url, headers=None):
    try:
        async with session.get(url, headers=headers, timeout=30) as response:
            response.raise_for_status()
            return await response.json()
    except Exception as e:
        print(f"ERRO ao buscar dados de {url}: {e}")
        return None

def inserir_no_banco(dados):
    """Função idêntica à do trigger_iha.py para salvar no banco de dados."""
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        query = f"""
            INSERT INTO {TABLE_DESTINO} (fk_id_iha, tipo_medicao, valor, data_hora)
            VALUES %s
            ON CONFLICT (fk_id_iha, tipo_medicao, data_hora) DO NOTHING
        """
        execute_values(cursor, query, dados)
        conn.commit()
        print(f" -> Sucesso! {len(dados)} registros do Adafruit inseridos no IHA.")

    except psycopg2.Error as e:
        print(f"Erro geral de Banco no Adafruit: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

async def sincronizar_adafruit():
    headers = {"X-AIO-Key": AIO_KEY}
    
    async with aiohttp.ClientSession() as session:
        # 1. Busca grupo para pegar as chaves dos feeds
        grupo = await fetch_json(session, GRUPO_URL, headers=headers)
        if not grupo:
            print("Não foi possível carregar o grupo.")
            return
        
        nome_ponto = grupo.get('name', 'Sem Nome')
        feeds = grupo.get("feeds", [])
        
        feed_metros = next((f for f in feeds if "metros" in f.get("key", "").lower()), None)
        feed_bateria = next((f for f in feeds if "bateria" in f.get("key", "").lower()), None)

        if not feed_metros or not feed_bateria:
            print("Feeds não encontrados.")
            return

        # 2. Busca o histórico de dados (limit=6 para manter o padrão do trigger_iha)
        url_base = f"https://io.adafruit.com/api/v2/{USERNAME}/feeds"
        url_metros = f"{url_base}/{feed_metros['key']}/data?limit=6"
        url_bateria = f"{url_base}/{feed_bateria['key']}/data?limit=6"

        print(f"Coletando dados Adafruit para: {nome_ponto} (ID: {ID_TOTEM_BANCO})...")
        
        dados_metros, dados_bateria = await asyncio.gather(
            fetch_json(session, url_metros, headers), 
            fetch_json(session, url_bateria, headers)
        )

        dados_para_inserir = []
        fuso_brasil = pytz.timezone('America/Sao_Paulo')

        # 3. Processa Metros e joga na lista de inserção
        if dados_metros:
            for item in dados_metros:
                try:
                    ts_utc = parser.isoparse(item["created_at"]) # Já entende que o 'Z' final é UTC
                    data_hora_brasil = ts_utc.astimezone(fuso_brasil)
                    
                    valor = max(0.0, float(item["value"])) # Garante que não é negativo
                    dados_para_inserir.append((ID_TOTEM_BANCO, 'metros', round(valor, 3), data_hora_brasil))
                except Exception:
                    pass

        # 4. Processa Bateria e joga na lista de inserção
        if dados_bateria:
            for item in dados_bateria:
                try:
                    ts_utc = parser.isoparse(item["created_at"])
                    data_hora_brasil = ts_utc.astimezone(fuso_brasil)
                    
                    valor = float(item["value"])
                    dados_para_inserir.append((ID_TOTEM_BANCO, 'bateria', round(valor, 2), data_hora_brasil))
                except Exception:
                    pass

        # 5. Executa a inserção em lote no banco
        if dados_para_inserir:
            inserir_no_banco(dados_para_inserir)
        else:
            print("Nenhum dado novo para inserir.")

if __name__ == "__main__":
    asyncio.run(sincronizar_adafruit())
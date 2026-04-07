import requests
import psycopg2
import os
import sys
import asyncio # <-- IMPORTANTE: Adicionado para rodar o Adafruit
from datetime import datetime, timezone, timedelta
from dateutil import parser
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Importa seus módulos auxiliares
from token_get import get_tokens
from trigger_iha import sincronizar_totens
from trigger_adafruit import sincronizar_adafruit # <-- IMPORTANTE: Importa o Adafruit

# Importa o thread para otimizar o codigo
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

# --- Configurações ---
API_ESTADO = "https://sws.cemaden.gov.br/PED/rest/pcds/pcds-dados-recentes"
API_ACUMULADOS = "https://sws.cemaden.gov.br/PED/rest/pcds-acum/acumulados-recentes"

LIMIT_REQ_POR_TOKEN = 12
FUSO_BR = timezone(timedelta(hours=-3))

# Mapeamento para a tabela de 'medicao' (Sensores de Estado)
SENSOR_MAPPING = {
    10: "pluviometria", 330: "nivel_1", 340: "nivel_2",
    350: "nivel_3", 360: "nivel_4", 610: "nivel_5", 620: "nivel_6",
}

def buscar_ids_cidades(conn):
    """Busca os IDs das cidades (que são os códigos IBGE)."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM cidades")
        return [row[0] for row in cur.fetchall()]

def processar_dados_estado(raw_data):
    """Processa dados do endpoint de Estado (medicao)."""
    dados_para_inserir = []
    for medicao in raw_data:
        try:
            sensor_id = medicao.get('id_sensor')
            tipo = SENSOR_MAPPING.get(sensor_id)
            if not tipo: continue
            
            cod = medicao.get('codestacao')
            val = medicao.get('valor')
            d_str = medicao.get('datahora')
            
            if not cod or val is None or not d_str: continue

            dt_utc = parser.parse(d_str).replace(tzinfo=timezone.utc)
            dt_br = dt_utc.astimezone(FUSO_BR)
            
            dados_para_inserir.append((cod, tipo, val, dt_br))
        except: continue
    return dados_para_inserir

def inserir_estado(conn, dados):
    if not dados: return
    with conn.cursor() as cur:
        q = """INSERT INTO cemadem_medicao (fk_codestacao, tipo_medicao, valor, data)
               VALUES %s ON CONFLICT ON CONSTRAINT medicao_unica DO NOTHING"""
        execute_values(cur, q, dados)
        conn.commit()
    print(f" [ESTADO] {len(dados)} medições inseridas.")

def inserir_acumulados(conn, dados):
    if not dados: return
    with conn.cursor() as cur:
        q = """INSERT INTO cemadem_acumulados 
               (fk_codestacao, fk_id_cidade, acc_1h, acc_3h, acc_6h, acc_12h, 
                acc_24h, acc_48h, acc_72h, acc_96h, acc_120h, data_hora)
               VALUES %s 
               ON CONFLICT (fk_id_cidade, fk_codestacao, data_hora) DO NOTHING"""
        execute_values(cur, q, dados)
        conn.commit()

def requisitar_cidade_acumulado(cod_ibge, token_ativo):
    """Função auxiliar isolada para fazer a requisição de uma cidade em paralelo."""
    h = {'accept': 'application/json', 'token': token_ativo}
    p = {'codibge': cod_ibge}
    
    try:
        r = requests.get(API_ACUMULADOS, headers=h, params=p, timeout=10)
        if r.status_code == 200:
            return cod_ibge, r.json(), None
        else:
            return cod_ibge, None, r.status_code
    except Exception as e:
        return cod_ibge, None, str(e)

def chamar_atualizacao_status_banco(conn):
    """Chama a função armazenada no banco de dados para atualizar o status das estações."""
    try:
        with conn.cursor() as cur:
            # Chama a função exata que você criou no PostgreSQL
            cur.execute("SELECT public.atualizar_status_estacoes();")
            conn.commit()
            print(" [ESTAÇÕES] Função de atualização de status executada com sucesso no banco.")
    except Exception as e:
        print(f" [ERRO] Falha ao chamar a função de status no banco: {e}")
        conn.rollback()


# --- FUNÇÃO PRINCIPAL ---
def main():
    conn = None
    try:
        # 1. Preparação
        conn = psycopg2.connect(DB_URL)
        tokens = get_tokens()
        
        if not tokens:
            print("CRÍTICO: Sem tokens disponíveis.")
            return

        # Variáveis de Controle de Token (Compartilhadas)
        idx_token = 0
        reqs_atuais = 0
        token_ativo = tokens[idx_token]

        print(f"\n=== 1. PROCESSANDO DADOS GERAIS DO ESTADO (PE) ===")
        # -----------------------------------------------------------
        # Faz 1 requisição para pegar o estado todo
        # -----------------------------------------------------------
        try:
            h = {'accept': 'application/json', 'token': token_ativo}
            p = {'rede': '11', 'uf': 'PE'}
            
            resp = requests.get(API_ESTADO, headers=h, params=p, timeout=15)
            reqs_atuais += 1 # CONTA +1 REQUISIÇÃO
            
            if resp.status_code == 200:
                dados_proc = processar_dados_estado(resp.json())
                inserir_estado(conn, dados_proc)
            else:
                print(f"Erro Estado: {resp.status_code}")
        except Exception as e:
            print(f"Erro Req Estado: {e}")

        
        print(f"\n=== 2. PROCESSANDO ACUMULADOS POR CIDADE ===")
        # -----------------------------------------------------------
        # Itera sobre as cidades usando paralelismo e lote de inserção
        # -----------------------------------------------------------
        cidades_ids = buscar_ids_cidades(conn)
        
        # 2.1 Pré-distribuir tokens para as cidades
        tarefas_cidades = []
        for cod_ibge in cidades_ids:
            if reqs_atuais >= LIMIT_REQ_POR_TOKEN:
                print(f" >> Token {idx_token+1} esgotado. Trocando...")
                idx_token += 1
                if idx_token >= len(tokens):
                    print("!!! SEM MAIS TOKENS !!! Parando alocação de acumulados.")
                    break
                token_ativo = tokens[idx_token]
                reqs_atuais = 0 # Reseta para o novo token
            
            tarefas_cidades.append((cod_ibge, token_ativo))
            reqs_atuais += 1

        batch_geral = []
        cidades_sucesso = 0
        
        # 2.2 Disparar as requisições em paralelo (10 workers simultâneos)
        with ThreadPoolExecutor(max_workers=10) as executor:
            futuros = [executor.submit(requisitar_cidade_acumulado, cod, tok) for cod, tok in tarefas_cidades]
            
            for futuro in as_completed(futuros):
                cod_ibge, l_dados, erro = futuro.result()
                
                if erro:
                    print(f" -> Cidade {cod_ibge}: Erro API {erro}")
                    continue
                    
                if l_dados:
                    cidades_sucesso += 1
                    for d in l_dados:
                        c_est = d.get('codestacao')
                        d_hora = d.get('datahora')
                        if c_est and d_hora:
                            # Parse data
                            try:
                                dto = parser.parse(d_hora)
                                if not dto.tzinfo: dto = dto.replace(tzinfo=timezone.utc)
                                dto = dto.astimezone(FUSO_BR)
                            except: dto = datetime.now()

                            batch_geral.append((
                                c_est, cod_ibge,
                                d.get('acc1hr',0), d.get('acc3hr',0), d.get('acc6hr',0),
                                d.get('acc12hr',0), d.get('acc24hr',0), d.get('acc48hr',0),
                                d.get('acc72hr',0), d.get('acc96hr',0), d.get('acc120hr',0),
                                dto
                            ))

        # 2.3 Única inserção de dados no banco (Batch Insert)
        if batch_geral:
            inserir_acumulados(conn, batch_geral)
            print(f" -> Sucesso nas requisições de {cidades_sucesso} cidades.")
            print(f"Total Acumulados Inseridos: {len(batch_geral)}")
        else:
            print("Total Acumulados Inseridos: 0 (Nenhum dado novo encontrado).")

        print(f"\n=== 3. SINCRONIZANDO TOTENS (IHA / THINGSPEAK) ===")
        sincronizar_totens()
        
        # ==========================================================
        # 3.1 GATILHO PARA ADAFRUIT
        # ==========================================================
        print(f"\n=== 3.1 SINCRONIZANDO TOTENS (IHA / ADAFRUIT) ===")
        # Executa a função assíncrona do arquivo trigger_adafruit.py
        asyncio.run(sincronizar_adafruit())

        # ==========================================================
        # 4. GATILHO PARA A FUNÇÃO DE STATUS (1x ao dia)
        # ==========================================================
        print(f"\n=== 4. VERIFICANDO STATUS DAS ESTAÇÕES ===")
        
        # Pega a hora atual no fuso do Brasil que você já definiu no topo do arquivo
        agora = datetime.now(FUSO_BR)
        
        # Como o worker roda a cada 5 min, ele só entra aqui na execução das 03:00
        if agora.hour == 1 and agora.minute < 5:
            print(" -> Horário de manutenção alcançado (03:00). Acionando o banco de dados...")
            chamar_atualizacao_status_banco(conn)
        else:
            print(f" -> Fora do horário de manutenção (Agora: {agora.strftime('%H:%M')}). Ignorando.")

    except Exception as e_geral:
        print(f"ERRO GERAL NO SCRIPT: {e_geral}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    main()
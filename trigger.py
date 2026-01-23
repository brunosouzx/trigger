import requests
import psycopg2
import sys
from datetime import datetime, timezone, timedelta
from dateutil import parser
from psycopg2.extras import execute_values
from token_get import get_token
from trigger_iha import sincronizar_totens

# --- Configurações da API ---
# Agora pegamos o estado todo, então não precisamos passar o 'codibge' vazio inicial
API_URL = "https://sws.cemaden.gov.br/PED/rest/pcds/pcds-dados-recentes"

# Pega o token dinamicamente do seu outro arquivo
API_TOKEN = get_token()

# MUDANÇA AQUI: Removemos o 'codibge' e deixamos fixo para PE
API_PARAMS = {
    'rede': '11',
    'uf': 'PE' 
}

# --- Configurações do Banco ---
db_url="postgresql://ihalagou_mq4l_user:1MK5i26pWskzRbjJZD4VK68JmHv3BGqH@dpg-d5pc847pm1nc73btfl50-a.virginia-postgres.render.com/ihalagou_mq4l"

SENSOR_MAPPING = {
    10: "pluviometria", 330: "nivel_1", 340: "nivel_2",
    350: "nivel_3", 360: "nivel_4", 610: "nivel_5", 620: "nivel_6",
}
fuso_gmt_menos_3 = timezone(timedelta(hours=-3))

def buscar_dados_estado_api(params):
    """
    Busca dados de TODAS as estações do estado configurado (PE).
    """
    print(f"Buscando dados RECENTES para o estado: {params['uf']}...")
    headers = {'accept': 'application/json', 'token': API_TOKEN}
    
    try:
        # A chamada agora é única e pode demorar um pouquinho mais para responder
        # pois traz muito mais dados, mas é muito mais eficiente que o loop.
        response = requests.get(API_URL, headers=headers, params=params)
        
        if response.status_code == 401:
            print("ERRO DE API: Falha na autenticação. Verifique seu TOKEN.")
            return []
            
        response.raise_for_status()
        response_data = response.json()

        if response_data:
            print(f" -> Sucesso! Recebidos {len(response_data)} registros do estado.")
            return response_data
        else:
            print(" -> Nenhum dado recente encontrado para o estado.")
            return []
            
    except requests.exceptions.RequestException as e:
        print(f"ERRO ao chamar a API: {e}")
        return []

def processar_dados(raw_data_list):
    """
    Transforma os dados brutos da API no formato do banco.
    """
    dados_para_inserir = []
    
    for medicao in raw_data_list:
        try:
            sensor_id = medicao.get('id_sensor')
            tipo_medicao = SENSOR_MAPPING.get(sensor_id)
            
            # Se não for um sensor que nos interessa (chuva ou nível), pula
            if tipo_medicao is None:
                continue 
            
            codestacao = medicao.get('codestacao')
            valor = medicao.get('valor')
            data_str = medicao.get('datahora')
            
            if not codestacao or valor is None or not data_str:
                continue

            # Conversão de fuso horário
            data_utc_obj = parser.parse(data_str).replace(tzinfo=timezone.utc)
            data_gmt3_obj = data_utc_obj.astimezone(fuso_gmt_menos_3)
            
            dados_para_inserir.append((
                codestacao, tipo_medicao, valor, data_gmt3_obj
            ))
        except (KeyError, TypeError, parser.ParserError) as e:
            continue 
            
    return dados_para_inserir

def inserir_no_banco(conn, dados_prontos):
    """
    Insere os dados no banco usando Bulk Insert.
    """
    if not dados_prontos:
        print("Nenhum dado válido para inserir após o processamento.")
        return

    try:
        with conn.cursor() as cur:
            # SQL Query
            query = """
                INSERT INTO cemadem_medicao 
                    (fk_codestacao, tipo_medicao, valor, data)
                VALUES %s
                ON CONFLICT ON CONSTRAINT medicao_unica DO NOTHING; 
            """
            
            # execute_values é muito rápido para muitos dados
            execute_values(cur, query, dados_prontos)
            conn.commit()
            
            print(f"Operação no banco concluída! {cur.rowcount} novos registros inseridos.")

    except psycopg2.Error as e:
        if conn: conn.rollback()
        
        # Tratamento de erro específico
        if "constraint \"medicao_unica\" does not exist" in str(e):
            print("ERRO CRÍTICO: A constraint 'medicao_unica' (para evitar duplicatas) não existe no banco.")
        elif e.pgcode == '23503': # Erro de Foreign Key
            print("ERRO DE CHAVE ESTRANGEIRA:")
            print("Você está tentando inserir medições de estações que NÃO estão cadastradas na tabela 'cemadem_estacao'.")
            print("Dica: Rode o script de atualização de estações (o anterior) para garantir que tem todas as estações de PE cadastradas.")
        else:
            print(f"ERRO de Banco desconhecido: {e}")

sincronizar_totens()

if __name__ == "__main__":
    conn = None
    try:
        # 1. Conecta ao Banco
        conn = psycopg2.connect(db_url)
        
        # 2. Busca dados da API (Agora uma única chamada para PE)
        todos_os_dados_brutos = buscar_dados_estado_api(API_PARAMS)
        
        # 3. Processa e Insere
        if todos_os_dados_brutos:
            print(f"Iniciando processamento...")
            dados_processados = processar_dados(todos_os_dados_brutos)
            
            if dados_processados:
                print(f"Identificados {len(dados_processados)} medições válidas (chuva/nível). Inserindo...")
                inserir_no_banco(conn, dados_processados)
            else:
                print("Os dados chegaram, mas nenhum corresponde aos sensores mapeados (Pluviometria/Nível).")
        else:
            print("Nenhum dado retornado.")

    except psycopg2.Error as e:
        print(f"ERRO GERAL de conexão com o banco: {e}")
    finally:
        if conn:
            conn.close()
            print("Conexão encerrada.")
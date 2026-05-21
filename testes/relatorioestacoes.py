import requests
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv() 
DB_CONFIG = os.getenv('DATABASE_URL')
URL_API = "https://geoportal.apac.pe.gov.br/server/rest/services/met_monitoramento_chuvas_pe/MapServer/4/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=horas_24%20desc&resultOffset=0&resultRecordCount=2000" 

def relatorio_auditoria_estacoes():
    if not DB_CONFIG:
        raise ValueError("A variável DATABASE_URL não foi encontrada.")

    conn = None
    try:
        # ==========================================
        # 1. BUSCAR DADOS DO BANCO DE DADOS
        # ==========================================
        print("Conectando ao banco de dados...")
        conn = psycopg2.connect(DB_CONFIG)
        cursor = conn.cursor()

        # Busca todas as estações e seus status
        cursor.execute("SELECT codestacao, ativo FROM cemadem_estacao WHERE codestacao IS NOT NULL;")
        
        ativas_bd = set()
        inativas_bd = set()

        for codestacao, ativo in cursor.fetchall():
            if ativo:
                ativas_bd.add(codestacao)
            else:
                inativas_bd.add(codestacao)

        print(f"BD: {len(ativas_bd)} ativas | {len(inativas_bd)} inativas.")

        # ==========================================
        # 2. BUSCAR DADOS DA API
        # ==========================================
        print("Buscando dados da API...")
        response = requests.get(URL_API)
        response.raise_for_status()
        features = response.json().get('features', [])
        
        # Dicionário para guardar o código e o nome da estação da API
        estacoes_api = {}
        for feature in features:
            atributos = feature.get('attributes', {})
            codigo_api = atributos.get('codigo_gmmc')
            nome = atributos.get('nome', 'Sem Nome')
            
            if codigo_api:
                estacoes_api[codigo_api] = nome
                
        codigos_api = set(estacoes_api.keys())
        print(f"API: {len(codigos_api)} estações recebidas.\n")

        # ==========================================
        # 3. CRUZAMENTO DE DADOS (A Mágica dos Sets)
        # ==========================================
        
        # Cenário 1: Inativas no BD que estão na API
        inativas_com_dados_api = inativas_bd.intersection(codigos_api)
        
        # Cenário 2: Ativas no BD que estão na API
        ativas_com_dados_api = ativas_bd.intersection(codigos_api)
        
        # Cenário 3: Estão na API, mas NÃO existem no seu Banco
        orfans_na_api = codigos_api.difference(ativas_bd.union(inativas_bd))
        
        # Cenário 4: Estão no seu Banco, mas sumiram da API
        orfans_no_bd = (ativas_bd.union(inativas_bd)).difference(codigos_api)

        # ==========================================
        # 4. EXIBIÇÃO DO RELATÓRIO
        # ==========================================
        print("=" * 60)
        print("📊 RELATÓRIO DE AUDITORIA: BANCO DE DADOS vs API")
        print("=" * 60)

        print(f"\n🔴 1. INATIVAS (ativo=false) que estão na API: {len(inativas_com_dados_api)}")
        for cod in inativas_com_dados_api:
            print(f"   -> Código: {cod} | Nome na API: {estacoes_api[cod]}")

        print(f"\n🟢 2. ATIVAS (ativo=true) que estão na API: {len(ativas_com_dados_api)}")
        # Comentado para não poluir a tela, mas você pode descomentar se quiser ver a lista:
        # for cod in ativas_com_dados_api:
        #     print(f"   -> Código: {cod} | Nome na API: {estacoes_api[cod]}")
        print("   (Lista omitida para não poluir o terminal)")

        print(f"\n⚠️ 3. ESTÃO NA API, MAS NÃO EXISTEM NO BANCO: {len(orfans_na_api)}")
        for cod in orfans_na_api:
            print(f"   -> Código: {cod} | Nome na API: {estacoes_api[cod]} (Precisa ser cadastrada!)")

        print(f"\n👻 4. ESTÃO NO BANCO, MAS SUMIRAM DA API: {len(orfans_no_bd)}")
        for cod in orfans_no_bd:
            print(f"   -> Código: {cod} (Verificar se a estação foi desativada permanentemente)")
            
        print("\n" + "=" * 60)

    except requests.exceptions.RequestException as e:
        print(f"Erro de comunicação com a API: {e}")
    except psycopg2.Error as e:
        print(f"Erro de banco de dados PostgreSQL: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    relatorio_auditoria_estacoes()
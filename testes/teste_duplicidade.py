import psycopg2
import os
from dotenv import load_dotenv

# Carrega a URL do banco do arquivo .env
load_dotenv()
DB_URL = os.getenv("DATABASE_URL")

def verificar_estacoes_duplicadas():
    print("=== VERIFICANDO ESTAÇÕES DUPLICADAS ===")
    conn = None
    cursor = None
    
    # 1. Defina as tabelas e as colunas que guardam o código da estação
    TABELA_1 = "public.apac_estacao"
    COLUNA_1 = "codestacao"
    
    TABELA_2 = "public.cemadem_estacao"
    COLUNA_2 = "codestacao"

    try:
        conn = psycopg2.connect(DB_URL)
        cursor = conn.cursor()

        # 2. A query usa INTERSECT para pegar apenas os códigos que existem em ambas as consultas
        query = f"""
            SELECT {COLUNA_1} FROM {TABELA_1}
            INTERSECT
            SELECT {COLUNA_2} FROM {TABELA_2}
        """
        
        cursor.execute(query)
        # Fetchall retorna uma lista de tuplas, pegamos o primeiro item (o código) de cada tupla
        estacoes_duplicadas = [linha[0] for linha in cursor.fetchall()]

        # 3. Exibe o resultado
        if estacoes_duplicadas:
            print(f"⚠️ Foram encontradas {len(estacoes_duplicadas)} estações presentes em AMBAS as tabelas:")
            for estacao in estacoes_duplicadas:
                print(f" -> {estacao}")
        else:
            print("✅ Nenhuma estação duplicada encontrada entre as tabelas analisadas.")

    except Exception as e:
        print(f"❌ Erro na consulta ao banco de dados: {e}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == "__main__":
    verificar_estacoes_duplicadas()
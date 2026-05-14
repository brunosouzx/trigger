import requests
import psycopg2
import os
from dotenv import load_dotenv # Importe a biblioteca para ler o .env

# Carrega as variáveis do arquivo .env para o ambiente do sistema
load_dotenv() 

# 1. Configurações de Conexão
# Usamos um fallback caso não encontre no .env para alertar rapidamente
DB_CONFIG = os.getenv('DATABASE_URL')

if not DB_CONFIG:
    raise ValueError("A variável DATABASE_URL não foi encontrada. Verifique seu arquivo .env")

# Substitua pela URL real da sua API
URL_API = "https://geoportal.apac.pe.gov.br/server/rest/services/met_monitoramento_chuvas_pe/MapServer/4/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=horas_24%20desc&resultOffset=0&resultRecordCount=2000" 

def sincronizar_estacoes_apac():
    try:
        # 2. Fazer a requisição para a API
        print("Buscando dados da API...")
        response = requests.get(URL_API)
        response.raise_for_status()
        dados_json = response.json()
        
        # Isolar apenas as "features"
        features = dados_json.get('features', [])
        print(f"{len(features)} estações encontradas na API.")

        # 3. Conectar ao banco de dados PostgreSQL
        print("Conectando ao banco de dados...")
        
        # Correção aqui: Passe a variável DIRETAMENTE, sem os **
        conn = psycopg2.connect(DB_CONFIG) 
        cursor = conn.cursor()

        # 4. Buscar os 'codestacao' que já existem para fazer a verificação
        # Usa UNION para garantir que não vamos duplicar algo que já está em cemadem_estacao ou na própria apac_estacao
        query_existentes = """
            SELECT codestacao FROM cemadem_estacao WHERE codestacao IS NOT NULL
            UNION
            SELECT codestacao FROM apac_estacao WHERE codestacao IS NOT NULL;
        """
        cursor.execute(query_existentes)
        
        # Usar um 'set' (conjunto) torna a verificação (in) em O(1), muito mais rápido
        estacoes_existentes = {linha[0] for linha in cursor.fetchall()}

        # 5. Preparar a query de inserção para a tabela apac_estacao
        query_insert = """
            INSERT INTO apac_estacao (fk_id_cidade, nome_estacao, latitude, longitude, ativo, codestacao)
            VALUES (%s, %s, %s, %s, %s, %s);
        """

        contador_insercoes = 0

        # 6. Iterar sobre os dados da API e processar
        for feature in features:
            atributos = feature.get('attributes', {})
            
            # Pegar o código de verificação
            codigo_gmmc = atributos.get('codigo_gmmc')

            # Se o código existir e NÃO estiver no nosso banco de dados
            if codigo_gmmc and codigo_gmmc not in estacoes_existentes:
                
                # Mapeamento dos campos conforme solicitado
                # O cod_municipio vem como string na API, mas na tabela é integer
                try:
                    fk_id_cidade = int(atributos.get('cod_municipio'))
                except (ValueError, TypeError):
                    fk_id_cidade = None # Tratamento de segurança caso venha vazio

                nome_estacao = atributos.get('nome')
                latitude = atributos.get('latitude')
                longitude = atributos.get('longitude')
                ativo = True
                
                # Executar a inserção
                cursor.execute(query_insert, (
                    fk_id_cidade, 
                    nome_estacao, 
                    latitude, 
                    longitude, 
                    ativo, 
                    codigo_gmmc
                ))

                # Adicionar o código recém inserido ao 'set' para evitar duplicatas
                # caso a própria API retorne a mesma estação duas vezes no mesmo JSON
                estacoes_existentes.add(codigo_gmmc)
                contador_insercoes += 1

        # 7. Efetivar as mudanças no banco de dados
        conn.commit()
        print(f"Sucesso! {contador_insercoes} novas estações inseridas na tabela apac_estacao.")

    except requests.exceptions.RequestException as e:
        print(f"Erro de comunicação com a API: {e}")
    except psycopg2.Error as e:
        print(f"Erro de banco de dados PostgreSQL: {e}")
        if 'conn' in locals() and conn:
            conn.rollback() # Desfaz a transação em caso de erro
    finally:
        # 8. Fechar os cursores e conexões adequadamente
        if 'conn' in locals() and conn:
            cursor.close()
            conn.close()
            print("Conexão com o banco fechada.")

if __name__ == "__main__":
    sincronizar_estacoes_apac()
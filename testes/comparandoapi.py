import requests

# Substitua pelas URLs reais das suas APIs
URL_API_1 = "https://geoportal.apac.pe.gov.br/server/rest/services/met_monitoramento_chuvas_pe/MapServer/4/query?f=json&where=1%3D1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=horas_24%20desc&resultOffset=0&resultRecordCount=2000" # Aquela que tem a estrutura "features"
URL_API_2 = "http://dados.apac.pe.gov.br:41120/cemaden/" # Aquela que é uma lista direta

def comparar_somente_apis():
    try:
        # ---------------------------------------------------------
        # 1. Buscar e limpar dados da API 1
        # ---------------------------------------------------------
        print("Buscando dados da API 1...")
        resp1 = requests.get(URL_API_1)
        resp1.raise_for_status()
        
        # Dicionário para limpar duplicatas internas da API 1
        estacoes_api1 = {}
        for feature in resp1.json().get('features', []):
            atributos = feature.get('attributes', {})
            codigo = atributos.get('codigo_gmmc')
            nome = atributos.get('nome')
            
            if codigo: # Se tiver código válido, salva/atualiza
                estacoes_api1[codigo] = nome

        # ---------------------------------------------------------
        # 2. Buscar e limpar dados da API 2
        # ---------------------------------------------------------
        print("Buscando dados da API 2...")
        resp2 = requests.get(URL_API_2)
        resp2.raise_for_status()
        
        # Dicionário para limpar duplicatas internas da API 2
        estacoes_api2 = {}
        for item in resp2.json():
            codigo = item.get('Codigo_gmmc')
            nome = item.get('Estação')
            
            if codigo:
                estacoes_api2[codigo] = nome

        # ---------------------------------------------------------
        # 3. A MÁGICA DA COMPARAÇÃO (Conjuntos Matemáticos)
        # ---------------------------------------------------------
        codigos_1 = set(estacoes_api1.keys())
        codigos_2 = set(estacoes_api2.keys())

        # Interseção: O que tem na 1 E na 2
        em_comum = codigos_1.intersection(codigos_2)
        
        # Diferença: O que tem em uma, mas não tem na outra
        exclusivas_api1 = codigos_1 - codigos_2
        exclusivas_api2 = codigos_2 - codigos_1

        # ---------------------------------------------------------
        # 4. Exibir o Relatório
        # ---------------------------------------------------------
        print("\n" + "=" * 60)
        print("📊 RELATÓRIO DE CRUZAMENTO: API 1 x API 2")
        print("=" * 60)
        print(f"Total de estações únicas na API 1: {len(codigos_1)}")
        print(f"Total de estações únicas na API 2: {len(codigos_2)}")
        print("-" * 60)
        print(f"✅ EM COMUM (Estão nas duas APIs): {len(em_comum)}")
        print(f"🔵 EXCLUSIVAS DA API 1: {len(exclusivas_api1)}")
        print(f"🟢 EXCLUSIVAS DA API 2: {len(exclusivas_api2)}")
        print("=" * 60)

        # Imprime a lista do que está duplicado nas duas APIs
        if em_comum:
            print("\nLista de estações presentes em AMBAS as APIs:")
            for codigo in em_comum:
                # Pega o nome da API 1 como referência
                nome = estacoes_api1[codigo] 
                print(f" -> Código: {codigo} | Nome: {nome}")

    except requests.exceptions.RequestException as e:
        print(f"Erro de comunicação com a internet/API: {e}")
    except ValueError as e:
        print(f"Erro processando o JSON: {e}")

if __name__ == "__main__":
    comparar_somente_apis()
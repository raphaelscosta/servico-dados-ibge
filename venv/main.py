from unidecode import unidecode
import requests
import os
import pandas as pd
from dotenv import load_dotenv
load_dotenv()

"""
Formatando dados Buscando padronizar: removendo acentos, colocando em letras minúsculas e tirando espaços
"""
def remover_formatacao(texto: str):
    return unidecode(str(texto)).lower().strip()


"""
- Buscando dados na API do iBGE. Usei o raise_for_status para caso a solicitação seja diferente de 200
Se for, ele lança uma exceção e eu posso mostrar o código dela para possíveis analises
"""
try:
    response = requests.get("https://servicodados.ibge.gov.br/api/v1/localidades/municipios", timeout=10)
    response.raise_for_status()
    ibgeData = response.json()
except Exception as e:
    print(f" ERRO NA API DO IBGE: {e}")
    ibgeData = [] 

dados_coletados_ibge = {}


if ibgeData:
    for dado in ibgeData:
        nome_formatado = remover_formatacao(dado["nome"])
        uf_data = ""
        regiao_nome = ""

        #Tentei fazer referência direta, mas gerou NoneType, então tive que descer nível por nivel
        micro = dado.get("microrregiao")
        if micro:
            meso = micro.get("mesorregiao")
            if meso:
                uf_data = meso.get("UF")
                if uf_data:
                    uf_sigla = uf_data.get("sigla", "")
                    regiao = uf_data.get("regiao")
                    if regiao:
                        regiao_nome = regiao.get("nome", "")


        """
        Em dados coletados estou pegando os dados do ibge formatados para fazer a comparação
        """
        dados_coletados_ibge[nome_formatado] = {
            'municipio_ibge': dado.get('nome'),
            'uf': uf_sigla,
            'regiao': regiao_nome,
            'id_ibge': dado['id']

        }

    """"
    Aqui faço um lista final para receber os dados comparados, se existirem
    Depois transformo esssa lista em um DataFrame para "tabelar" meus dados 
    """
    """
Aqui faço um lista final para receber os dados comparados...
"""
lista_final = []
base_nao_encontrado_ou_erro = {"municipio_ibge": "", "uf": "", "regiao": "", "id_ibge": ""}

# Leitura do CSV
csv = pd.read_csv("input.csv")
for i, linha in csv.iterrows():
    nome_original = linha["municipio"]
    pop_original = linha["populacao"]
    nome_busca = remover_formatacao(nome_original)

    if not dados_coletados_ibge:
        info = base_nao_encontrado_ou_erro
        status = "ERRO_API"
    elif nome_busca in dados_coletados_ibge:
        info = dados_coletados_ibge[nome_busca]
        status = "OK"
    else:
        info = base_nao_encontrado_ou_erro
        status = "NAO_ENCONTRADO"

    lista_final.append({
        "municipio_input": nome_original,
        "populacao_input": pop_original,
        "municipio_ibge": info["municipio_ibge"],
        "uf": info["uf"],
        "regiao": info["regiao"],
        "id_ibge": info["id_ibge"],
        "status": status
    })

df_resultado = pd.DataFrame(lista_final)

"""
Exportando para CSV com a codificação correta
"""
df_resultado.to_csv("resultado_final.csv", index=False, encoding='utf-8-sig', sep=';')

"""
Estatísticas com Pandas -> Aqui tive que recorrer a IA pois meu conhecimentos em pandas não suprem o que foi pedido
"""
total_de_municipios = len(df_resultado)
total_ok = (df_resultado['status'] == 'OK').sum()
total_nao_encontrado = (df_resultado['status'] == 'NAO_ENCONTRADO').sum()
total_erro_api = (df_resultado['status'] == 'ERRO_API').sum() 
pop_total_ok = df_resultado.loc[df_resultado['status'] == 'OK', 'populacao_input'].sum()
medias_brutas = df_resultado.loc[df_resultado['status'] == 'OK'].groupby('regiao')['populacao_input'].mean().to_dict()
medias_por_regiao = {regiao: float(round(valor, 2)) for regiao, valor in medias_brutas.items()}

json_status = {
    "stats": {
        "total_municipios": int(total_de_municipios),
        "total_ok": int(total_ok),
        "total_nao_encontrado": int(total_nao_encontrado),
        "total_erro_api": int(total_erro_api),
        "pop_total_ok": int(pop_total_ok),
        "medias_por_regiao": medias_por_regiao 
    }
}



"""
Aqui também recorri a materias externos  pois nunca havia requisição post da forma pedida 
"""
url = "https://mynxlubykylncinttggu.functions.supabase.co/ibge-submit"
token_novo = os.getenv(ACCESS_TOKEN)

headers = {
    "Authorization": f"Bearer {token_novo}",
    "Content-Type": "application/json"
}

try:
    resposta = requests.post(url, headers=headers, json=json_status)
    resposta.raise_for_status() 
    
    resultado = resposta.json()
    print(f"{resultado.get('score')}")
    print(f"{resultado.get('message')}")

except requests.exceptions.HTTPError as err:
    print(f"Erro retornado pelo servidor: {err}")
    print(f"Detalhes: {err.response.text}") 
except Exception as e:
    print(f"Falha no envio: {e}")
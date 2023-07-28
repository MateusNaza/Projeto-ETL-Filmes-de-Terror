# %%
import json
import boto3
import datetime
import requests


def lambda_handler(event, context):
    # ---- Leitura da chave de api --------
    with open('../1 - Tarefa/key.txt') as file:
        apiKey = file.read()

    ### ------------- Extração de dados dos filmes similares ----------------- ###
    # ---- Declaração de variáveis --------
    url_filmes = f"https://api.themoviedb.org/3/movie/747/similar?api_key={apiKey}&language=pt-BR"
    pagina = 1
    resultados = []
    total_paginas = 1
    
    # ---- Laço para obter os dados de todas as páginas ----------
    while pagina <= total_paginas:
        response = requests.get(url_filmes + f'&page={pagina}')
        data = response.json()
        resultado_list = [item for item in data['results'] if 27 in item['genre_ids']]
        resultados.extend(resultado_list)
        total_paginas = data['total_pages']
        pagina += 1
    
    # ---- Iteração sobre os resultados para definir as chaves ----------
    filmes = list(map(lambda filme: {
        'Id': filme['id'],
        'Filme': filme['title'],
        'Titulo Original': filme['original_title'],
        'Genero Id': filme['genre_ids'],
        'Visao Geral': filme['overview'],
        'Popularidade': filme['popularity'],
        'Data de lancamento': filme['release_date'],
        'Media de votos:': filme['vote_average'],
        'Contagem de votos': filme['vote_count']
    }, resultados))
    
    # ---- Transformando a lista de filmes para o formato JSON -----------
    filmes_json = json.dumps(filmes, indent=4, ensure_ascii=False)
    
    ### -------- Extração de dados para identificar o id_ do genero com seu respectivo nome -------- ###
    
    # ---- Declaração de variáveis --------
    url_generos = f"https://api.themoviedb.org/3/genre/movie/list?api_key={apiKey}&language=pt-BR"
    
    # ---- Consumindo os dados -----------
    response = requests.get(url_generos)
    data = response.json()
    
    # ---- Iteração sobre os resultados para definir as chaves ----------
    generos = list(map(lambda gen: {'Id': gen['id'], 'Genero': gen['name']}, data['genres']))
    
    # ---- Transformando a lista de generos para o formato JSON -----------
    generos_json = json.dumps(generos, indent=4, ensure_ascii=False)
    
    # ---- Configurando o cliente e Declarando variáveis --------
    s3 = boto3.client('s3')
    bucket_name = 'data-lake-mateus-naza'  
    date = datetime.datetime.now()
    object_dict = {filmes_json: 'filmes_relacionados_terror.json', generos_json: 'lista_generos.json'}
    
    # ---- Gravação no S3 -------------
    for content, obj in object_dict.items():
        file_path = 'RAW/TMDB/JSON/{}/{}/{}/{}'\
                    .format(date.year, date.month, date.day, obj)
        
        try:
            s3.put_object(
                Body=content,
                Bucket=bucket_name,
                Key=file_path
            )

        except Exception as e:
            return {
                'statusCode': 500,
                'body': f'Erro ao gravar arquivo JSON no S3: {str(e)}'
            }
    return {
        'statusCode': 200,
        'body': 'Arquivo JSON gravado com sucesso no S3'
    }


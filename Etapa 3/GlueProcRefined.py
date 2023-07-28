import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, explode, floor, monotonically_increasing_id

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_TMDB_PATH', 'S3_LOCAL_PATH', 'S3_REFINED_PATH'])


#---- Inicialização de variáveis ----

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#---- Criação do DataFrame dinamico do TMDB ----

df_dynamic_tmdb = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            args['S3_TMDB_PATH']  
        ]
    },
    "parquet",

)


#---- Criação do DataFrame dinamico do Local ----

df_dynamic_local = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            args['S3_LOCAL_PATH']
            ],
        "recurse": True,
        "groupFiles": "inPartition"
    },
    format="parquet"
)


#---- Conversão para DataFrame Spark----

df_movies_tmdb = df_dynamic_tmdb.toDF()
df_movies_local = df_dynamic_local.toDF()


#---- Criação da Coluna Decada ----
df_movies_local = df_movies_local.withColumn('Decada_Lancamento',
                                             floor(df_movies_local['Ano_Lancamento'] / 10))


#---- Junção dos DataFrames ----
df_joined = df_movies_local.join(df_movies_tmdb, 'Titulo_Original')


#---- Transformando coluna Decada_Lancamento para inteiro ----
df_joined = df_joined.withColumn('Decada_Lancamento', col('Decada_Lancamento').cast('int'))


#---- 'Explodindo' coluna de genero ----
df_joined = df_joined.select(
    'Nome_Filme', 'Titulo_Original', 'Ano_Lancamento',
    'Nota_Media', 'Sexo', 'Nome_Artista', 'Ano_Nascimento',
    'Ano_Falecimento', explode('Id_Subgenero').alias('Id_Subgenero'),
    'Contagem_Votos','Decada_Lancamento', 'Popularidade'
)


#---- Filtragem para retornar somente generos que não são Terror ----
df_joined = df_joined.filter(df_joined['Id_Subgenero'] != 27)


#---- Criação dos DataFrames para cada Tabela ----

#---- Tabela Subgenero ---- 
genre_map = {
    28: "Ação", 12: "Aventura", 16: "Animação", 35: "Comédia", 80: "Crime", 99: "Documentário",
    18: "Drama", 10751: "Família", 14: "Fantasia", 36: "História", 10402: "Música",
    9648: "Mistério", 10749: "Romance", 878: "Ficção científica", 10770: "Cinema TV",
    53: "Thriller", 10752: "Guerra", 37: "Faroeste"
}
df_dim_subgenero = spark.createDataFrame(genre_map.items(), ["Id", "Genero"])
df_dim_subgenero = df_dim_subgenero.withColumn('Id', col('Id').cast('int'))


#---- Tabela Filme ----
df_dim_filme = df_joined.select(
    'Nome_Filme', 'Titulo_Original'
).dropDuplicates(['Nome_Filme'])

df_dim_filme = df_dim_filme.withColumn('Id', monotonically_increasing_id().cast('int') + 1)


#---- Tabela Data ----
df_dim_data = df_joined.select(
    'Ano_Lancamento', 'Decada_Lancamento'
).dropDuplicates(['Ano_Lancamento'])

df_dim_data = df_dim_data.withColumn('Id', monotonically_increasing_id().cast('int') + 1)


#---- Tabela Artista ----
df_dim_artista = df_joined.select(
    'Nome_Artista', 'Sexo', 'Ano_Nascimento', 'Ano_Falecimento'
).dropDuplicates(['Nome_Artista'])

df_dim_artista = df_dim_artista.withColumn('Id', monotonically_increasing_id()
                                           .cast('int') + 1)


#---- Tabela Fato ----
df_fato_filme_subgenero_artista = df_joined.join(
    df_dim_filme,
    df_joined.Nome_Filme == df_dim_filme.Nome_Filme,
    'left'
).join(
    df_dim_artista,
    df_joined.Nome_Artista == df_dim_artista.Nome_Artista,
    'left'
).join(
    df_dim_data,
    df_joined.Ano_Lancamento == df_dim_data.Ano_Lancamento,
    'left'
).select(
    df_joined.Nota_Media,
    df_joined.Popularidade,
    df_joined.Contagem_Votos,
    df_dim_filme.Id.alias('Id_Filme'),
    df_joined.Id_Subgenero,
    df_dim_artista.Id.alias('Id_Artista'),
    df_dim_data.Id.alias('Id_Data')
)

df_fato_filme_subgenero_artista = df_fato_filme_subgenero_artista.withColumn(
    'Id', monotonically_increasing_id().cast('int') + 1
)



#---- Gravação no S3 ----
output_path = args['S3_REFINED_PATH']

df_dim_subgenero.write.parquet(output_path + "/dim_subgenero")
df_dim_filme.write.parquet(output_path + "/dim_filme")
df_dim_data.write.parquet(output_path + "/dim_data")
df_dim_artista.write.parquet(output_path + "/dim_artista")
df_fato_filme_subgenero_artista.write.parquet(output_path + "/fato_filme_subgenero_artista")


job.commit()
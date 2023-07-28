import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, regexp_replace


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_RAW_PATH', 'S3_TRUSTED_PATH'])

#---- Inicialização de variáveis ----
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#---- Criação do DataFrame dinamico ----
df_dynamic_movies = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [args['S3_RAW_PATH'] + 'filmes_relacionados_terror.json'],
        "recurse": True
    },
    "json"
)

#---- Conversão para DataFrame Spark----
df_movies = df_dynamic_movies.toDF()

#---- Seleção e renomeação das colunas que serão utilizadas ----
df_movies = df_movies.select(
    col('Filme').alias('Nome_Filme'), col('Titulo Original').alias('Titulo_Original'),
    col('Genero Id').alias('Id_Subgenero'), col('Contagem de votos').alias('Contagem_Votos'), 'Popularidade'
)
    
#---- Limpeza de dados ----
df_movies = df_movies.filter(regexp_replace(col('Titulo_Original'), '[^a-zA-Z]', '') != '')

#---- Removendo filmes duplicados ----
df_movies = df_movies.dropDuplicates(['Nome_Filme'])

#---- Verificação nos Logs ----
df_movies.show()
df_movies.printSchema()

#---- Gravação no S3 ----
df_movies.write.mode("overwrite").parquet(args['S3_TRUSTED_PATH'])

job.commit()



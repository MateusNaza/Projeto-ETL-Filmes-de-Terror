import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, when

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_RAW_PATH', 'S3_TRUSTED_PATH'])


#---- Inicialização de variáveis ----
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
    

#---- Criação do DataFrame dinamico ----
df_dynamic = glueContext.create_dynamic_frame.from_options(
    "s3",
    {
        "paths": [
            args['S3_RAW_PATH']  # Criei um Job Parameter com essa variável para acessar o arquivo no S3
        ]
    },
    "csv",
    {"withHeader": True, "separator": "|"},

)

#---- Conversão para DataFrame Spark----
df_movies_csv = df_dynamic.toDF()

#---- Limpeza de Dados -----
df_movies_csv = df_movies_csv.na.replace('\\N', None).dropna()

#---- Seleção e reomeação de colunas -----
df_movies_csv = df_movies_csv.select(
    col('tituloOriginal').alias('Titulo_Original'), col('anoLancamento').alias('Ano_Lancamento'),
    col('notaMedia').alias('Nota_Media'), col( 'generoArtista').alias( 'Sexo'),
    col('nomeArtista').alias('Nome_Artista'),
    col('anoNascimento').alias('Ano_Nascimento'), col('anoFalecimento').alias('Ano_Falecimento')
)

#---- Removendo Nome de Artista Repetido -----
df_movies_csv = df_movies_csv.dropDuplicates(['Nome_Artista'])
'''Obs.: Essa etapa é muito importante pois faz parte da regra de negocio que eu criei para
um mesmo artista não estar presente em mais de um filme'''


#---- Alteração dos valores da coluna Sexo para M e F -----
df_movies_csv = df_movies_csv.withColumn('Sexo', when(df_movies_csv['Sexo'] == 'actor', 'M')\
                                         .when(df_movies_csv['Sexo'] == 'actress', 'F')\
                                         .otherwise(df_movies_csv['Sexo']))

#---- Adequando Schema das colunas numéricas ----
df_movies_csv = df_movies_csv.withColumn('Ano_Lancamento', col('Ano_Lancamento').cast('int'))
df_movies_csv = df_movies_csv.withColumn('Ano_Nascimento', col('Ano_Nascimento').cast('int'))
df_movies_csv = df_movies_csv.withColumn('Nota_Media', col('Nota_Media').cast('double'))
df_movies_csv = df_movies_csv.withColumn('Ano_Falecimento', col('Ano_Falecimento').cast('int'))

#---- Verificação nos Logs ----
df_movies_csv.printSchema()
df_movies_csv.show(5)
print(df_movies_csv.count())

#---- Gravação no S3 ----
df_movies_csv.write.mode("overwrite").parquet(args['S3_TRUSTED_PATH'])

job.commit()
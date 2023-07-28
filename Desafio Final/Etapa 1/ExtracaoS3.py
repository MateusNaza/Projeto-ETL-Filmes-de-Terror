# %%
import datetime
import boto3

# Criando sessão atravéz do profile_name que foi digitado na configuração do SSO
session = boto3.Session(profile_name="Mateus")

# Variável que apresenta o serviço aws
s3 = session.client('s3')

# Função de Carregamento no S3
def upload_file(bucket):
    categories = ['Movies', 'Series']
    date = datetime.datetime.now()

    for category in categories:
        object_name = f"{category.lower()}.csv"
        outpath = f"RAW/Local/CSV/{category}/{date.year}/{date.month}\
        /{date.day}/{object_name}"
         
        print(f'Iniciando carregamento em {outpath}')
        s3.upload_file(object_name, bucket, outpath)
        print('Carregamento finalizado....')

    print("Execução finalizada")
    return True

if __name__ == '__main__':
    upload_file("data-lake-mateus-naza")

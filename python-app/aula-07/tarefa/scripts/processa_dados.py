import os
import glob
import pandas as pd
import boto3
import botocore
import json
import shutil

from time import sleep
from kafka import KafkaConsumer
from json import loads
from datetime import datetime
from io import StringIO

# Método para salvar no S3/MinIO
def save_key_to_s3(data_frame, key):
    csv_buffer = StringIO()
    csv = data_frame.to_csv(csv_buffer, index=False)
    client.put_object(Body=csv_buffer.getvalue(), Bucket='aula-07', Key=key)
    response = client.get_object(Bucket='aula-07', Key=key)
    return response

# Crie o cliente com S3/MinIO
client = boto3.client('s3', 
    endpoint_url='http://awari-minio-nginx:9000',
    aws_access_key_id='NbJOHZE0DACAvnhd', #susbstitua por sua access_key_id
    aws_secret_access_key='KICxRAnE6NOR2ruSKZY6ONlqyHiPoy64', #susbstitua por sua secret_access_key
    aws_session_token=None,
    config=boto3.session.Config(signature_version='s3v4'),
    verify=False,
    region_name='sa-east-1'
)

# Crie um "consumer" Kafka
consumer = KafkaConsumer(
    'aula07-cidades',
     bootstrap_servers=['awari-kafka:9093'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='data-pipeline',
     value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Comece a ler mensagens do tópico Kafka
for message in consumer:
    # A mensagem conterá o nome do arquivo para processar
    message = message.value
    file_name = message['file_name'] # Extraia o nome do arquivo da mensagem
    print(f"Recebido novo arquivo para processar: {file_name}")

    # Leia os dados do arquivo
    data = pd.read_csv(f"../municipios-estados/{file_name}")
    uf = data['uf'].drop_duplicates().values[0]
    

    # Verifique se o arquivo CSV já existe no bucket para a UF correspondente
    key = f'tarefa/{uf}/cidades.csv'
    try:
        response = client.get_object(Bucket='aula-07', Key=key)
    except botocore.exceptions.ClientError as e:
        # Se o arquivo não existir, faça o upload de um vazio
        if e.response['Error']['Code'] == "NoSuchKey":
            empty_cidades_df = pd.read_csv("/home/awari/app/aula-07/tarefa/scripts/cidades.csv")
            save_key_to_s3(empty_cidades_df, key)

    # Leia os dados existentes do bucket
    existing_data = pd.read_csv(client.get_object(Bucket='aula-07', Key=key)['Body'])
    # Anexe novos dados aos existentes
    updated_data = pd.concat([existing_data, data])

    # Salve os dados atualizados de volta no bucket
    save_key_to_s3(updated_data, key)
    
    # Movendo arquivo processado para a subpasta "processed"
    processed_folder = "../municipios-estados/processed"
    if not os.path.exists(processed_folder):
        os.mkdir(processed_folder)

    shutil.move(f"../municipios-estados/{file_name}", f"{processed_folder}/{file_name}")
    print(f"Arquivo processado e movido: {file_name}")

    # "sleep"" se você quiser simular um atraso
    #sleep(10)
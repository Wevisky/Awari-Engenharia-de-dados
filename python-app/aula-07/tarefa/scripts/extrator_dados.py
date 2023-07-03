import pandas as pd
from time import sleep
from json import dumps
from kafka import KafkaProducer
from pymongo import MongoClient
import os

# Inicializa o cliente MongoDB
client_mongodb = MongoClient("mongodb://root:rootpassword@awari-mongodb:27017/?serverSelectionTimeoutMS=5000&connectTimeoutMS=10000&authSource=admin&authMechanism=SCRAM-SHA-256")
db = client_mongodb['exercicios']

# Inicializa o "producer" Kafka
producer = KafkaProducer(bootstrap_servers=['awari-kafka:9093'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))

# Diretório para salvar arquivos CSV
output_dir = "../municipios-estados/"

# Linhas por arquivo (para simular inserção de arquivos em momento diferentes)
rows_per_file = 100

# Itera sobre as coleções MongoDB

collection = db['cidades']

# Busca todos os documentos da coleção
data = list(collection.find())

# Converte dados para DataFrame
df = pd.DataFrame(data)

# Divide o DataFrame em DataFrames menores com "linhas_per_file" linhas cada
chunks = [df[i:i+rows_per_file] for i in range(0, df.shape[0], rows_per_file)]

# Salva cada chunk em um arquivo CSV diferente e envia seu nome para o Kafka
for i, chunk in enumerate(chunks):
    # Salva o chunk em arquivo CSV
    file_name = f"cidades_{i}.csv"
    file_path = os.path.join(output_dir, file_name)
    chunk.to_csv(file_path, index=False)
    
    # Envia o nome do arquivo para o Kafka
    producer.send('aula07-cidades', value={'file_name': file_name})

    # "sleep"" se você quiser simular um atraso
    #sleep(10)
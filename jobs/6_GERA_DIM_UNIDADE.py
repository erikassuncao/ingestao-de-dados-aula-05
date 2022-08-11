import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import boto3
import zlib

# realiza a leitura do arquivo dados_api.csv contendo os dados
dados = pd.read_csv("s3://trusted-dados-instituicoes-financeiras/dados_api.csv",sep = ';',encoding='iso-8859-1')

data = []
data.append(["CODIGO_UNIDADE","UNIDADE"])

for i, instituicao in dados.iterrows():
  if instituicao['CNPJ'] != " ":
    data.append([zlib.crc32(instituicao["Unidade"].encode()),instituicao['Unidade']])

df = pd.DataFrame(data)
df.drop_duplicates(subset='UNIDADE', inplace=True)

# Grava o resultado no bucket
df.to_csv("s3://trusted-dados-instituicoes-financeiras/dim_unidade.csv", sep=";", header=False, index=False,encoding='iso-8859-1')
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
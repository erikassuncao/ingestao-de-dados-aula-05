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
data.append(["CNPJ","CODIGO_SERVICO","CODIGO_UNIDADE","DATA_VIGENCIA","VALOR_MAXIMO","CODIGO_TIPO_VALOR"])

for i, instituicao in dados.iterrows():
  if instituicao['CNPJ'] != " ":
    codigo_tipo_valor = 1 if instituicao["TipoValor"] == "Real" else 2
    data.append([instituicao["CNPJ"],zlib.crc32(instituicao["Servico"].encode()),zlib.crc32(instituicao["Unidade"].encode()),instituicao["DataVigencia"],instituicao["ValorMaximo"],codigo_tipo_valor])

df = pd.DataFrame(data)
df.drop_duplicates(subset=None, inplace=True)

# Grava o resultado no bucket
df.to_csv("s3://trusted-dados-instituicoes-financeiras/fato_tarifas.csv", sep=";",header=False,index=False,encoding='iso-8859-1')
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
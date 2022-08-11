import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import boto3

# realiza a leitura do arquivo dados_api.csv contendo os dados
dados = pd.read_csv("s3://trusted-dados-instituicoes-financeiras/unificados.csv",sep = ';',encoding='iso-8859-1')

data = []
data.append(["CODIGO_TEMPO","CNPJ","INDICE","QTD_RECLAMACOES_PROCEDENTES","QTD_RECLAMACOES_OUTRAS","QTD_RECLAMACOES_NAO_REGULADAS","QTD_RECLAMACOES","QTD_CLIENTES","QTD_CLIENTES_CCS","QTD_CLIENTES_SCR"])

for i, instituicao in dados.iterrows():
  if instituicao['CNPJ'] != " ":
    if instituicao['Indice'] == " ": 
      instituicao['Indice'] = 0
    if instituicao['Quantidade de clientes CCS'] == " ": 
      instituicao['Quantidade de clientes CCS'] = 0
    if instituicao['Quantidade de clientes SCR'] == " ": 
      instituicao['Quantidade de clientes SCR'] = 0  
    if instituicao['Quantidade total de clientes CCS e SCR'] == " ":
      instituicao['Quantidade total de clientes CCS e SCR'] = 0
    data.append([str(instituicao['Ano']) + str(instituicao['Trimestre'])[:-2],instituicao['CNPJ'],str(instituicao['Indice']).replace(".","").replace(",","."),instituicao['Quantidade de reclamaÃ§Ãµes reguladas procedentes'],instituicao['Quantidade de reclamaÃ§Ãµes reguladas - outras'],instituicao['Quantidade de reclamaÃ§Ãµes nÃ£o reguladas'],instituicao['Quantidade total de reclamaÃ§Ãµes'],instituicao['Quantidade total de clientes CCS e SCR'],instituicao['Quantidade de clientes CCS'],instituicao['Quantidade de clientes SCR']])

df = pd.DataFrame(data)

df.drop_duplicates(subset=None, inplace=True)

# Grava o resultado no bucket
df.to_csv("s3://trusted-dados-instituicoes-financeiras/fato_recl.csv", sep=";",header=False,index=False,encoding='iso-8859-1')
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
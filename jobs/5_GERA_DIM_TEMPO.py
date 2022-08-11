import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import boto3

# realiza a leitura do arquivo unificados.csv contendo os CNPJ
dados = pd.read_csv("s3://trusted-dados-instituicoes-financeiras/unificados.csv",sep = ';',encoding='iso-8859-1')

data = []
data.append(["ANO","TRIMESTRE","CODIGO_TEMPO"])
for i, instituicao in dados.iterrows():
  if instituicao['CNPJ'] != " ":
    data.append([instituicao['Ano'],instituicao['Trimestre'],str(instituicao['Ano']) + str(instituicao['Trimestre'])[:-2]])

df = pd.DataFrame(data)
df.drop_duplicates(subset=None, inplace=True)
# Grava o resultado no arquivo DIM_TEMPO
df.to_csv('s3://trusted-dados-instituicoes-financeiras/dim_tempo.csv',header=False,index=False, sep=";", encoding='iso-8859-1')
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.commit()
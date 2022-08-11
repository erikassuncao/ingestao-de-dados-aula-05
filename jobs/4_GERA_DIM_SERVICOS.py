import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import urllib.request, json 
import boto3
import csv


# realiza a leitura do arquivo dados_api.csv contendo os dados
dados = pd.read_csv("s3://trusted-dados-instituicoes-financeiras/dados_api.csv",sep = ';',encoding='iso-8859-1')

csv_file =  open("s3://trusted-dados-instituicoes-financeiras/dim_servico.csv", 'w')
csv_writer = csv.writer(csv_file, delimiter=";")

csv_writer.writerow(["CODIGO_SERVICO","SERVICO"])

for i, servico in dados.iterrows():
    csv_writer.writerow([servico['CodigoServico'],servico['Servico']])

csv_file.close()

df = pd.read_csv("s3://trusted-dados-instituicoes-financeiras/dim_servico.csv", sep=";")

df.drop_duplicates(subset=["CODIGO_SERVICO","SERVICO"], inplace=True)
ordenado = df.sort_values(by=["CODIGO_SERVICO"], ascending=True)

# Write the results to a different file
ordenado.to_csv("s3://trusted-dados-instituicoes-financeiras/dim_servico.csv", sep=";", index=False,encoding='iso-8859-1')
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.commit()
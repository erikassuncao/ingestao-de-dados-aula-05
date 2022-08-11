import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# importa as bibliotecas necessrias
import boto3
import pandas as pd
import csv

# realiza a leitura do arquivo unificado.csv contendo os CNPJ
dados = pd.read_csv("s3://trusted-dados-instituicoes-financeiras/unificados.csv",sep = ';',encoding='iso-8859-1')

csv_file =  open("s3://trusted-dados-instituicoes-financeiras/dim_instituicao.csv", 'w')
csv_writer = csv.writer(csv_file, delimiter=";")

csv_writer.writerow(["CNPJ","Instituicao","Tipo"])

for i, instituicao in dados.iterrows():
  if instituicao['CNPJ'] != " ":
    csv_writer.writerow([instituicao['CNPJ'],instituicao['Instituicao'],instituicao['Tipo']])

csv_file.close()

df = pd.read_csv("s3://trusted-dados-instituicoes-financeiras/dim_instituicao.csv", sep=";")

df.drop_duplicates(subset=None, inplace=True)
ordenado = df.sort_values(by=["Instituicao"], ascending=True)

# Grava resultado tratado
ordenado.to_csv("s3://trusted-dados-instituicoes-financeiras/dim_instituicao.csv", sep=";", encoding='iso-8859-1', index=False)


  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.commit()
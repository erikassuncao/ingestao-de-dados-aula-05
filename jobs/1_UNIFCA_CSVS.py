import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import s3fs
import pandas as pd
import boto3

client = boto3.client('s3')
keys = []

bucket = 'raw-dados-instituicoes-financeiras'

resp = client.list_objects_v2(Bucket = bucket)

for i in resp['Contents']:
    if ('prefix' in i['Key']) & ('.csv' in i['Key']):
        n = "s3://" + bucket + "/"+ i['Key']
        keys.append(n)

data = pd.concat([pd.read_csv((k)) for k in keys])

cabecalho = ["Ano","Trimestre","Categoria","Tipo","CNPJ","Instituicao","Indice","Quantidade de reclamações reguladas procedentes","Quantidade de reclamações reguladas - outras","Quantidade de reclamações não reguladas","Quantidade total de reclamações","Quantidade total de clientes CCS e SCR","Quantidade de clientes CCS","Quantidade de clientes SCR",""]

data.to_csv("s3://trusted-dados-instituicoes-financeiras/" + "unificados.csv", index=False, index_col=None, sep=';', skiprows=[0], header=cabecalho, encoding ='ISO-8859-1')
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.commit()
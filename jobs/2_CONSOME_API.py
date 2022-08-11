import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import urllib.request, json 
import boto3

# inicializa a lista de tarifas
tarifas = []

# realiza a leitura do arquivo unificado.csv contendo os CNPJ
instituicoes = pd.read_csv("s3://trusted-dados-instituicoes-financeiras/unificados.csv",sep = ';',encoding='iso-8859-1')

# loop nos CNPJ do arquivo CSV e repasse ao parametro da API
for instituicao in instituicoes['CNPJ']:
    if instituicao != " ":
      with urllib.request.urlopen("https://olinda.bcb.gov.br/olinda/servico/Informes_ListaTarifasPorInstituicaoFinanceira/versao/v1/odata/ListaTarifasPorInstituicaoFinanceira(PessoaFisicaOuJuridica=@PessoaFisicaOuJuridica,CNPJ=@CNPJ)?@PessoaFisicaOuJuridica='J'&@CNPJ='" + str(instituicao) + "'&$top=100&$format=json&$select=CodigoServico,Servico,Unidade,DataVigencia,ValorMaximo,TipoValor,Periodicidade") as url:
        conteudo = json.loads(url.read().decode())['value']
        for temp in conteudo:
          # adiciona CNPJ ao objeto para que o CSV posteriormente tenha relacionamento
          temp.update({"CNPJ":instituicao})
          tarifas.append(temp)

# Salva o conteudo JSON da API no formato CSV na pasta trusted 
df = pd.DataFrame(tarifas, columns=["CodigoServico","Servico","Unidade","DataVigencia","ValorMaximo","TipoValor","Periodicidade","CNPJ"])
df.to_csv('s3://trusted-dados-instituicoes-financeiras/dados_api.csv',sep = ';', index=False)
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.commit()
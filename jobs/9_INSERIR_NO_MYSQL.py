import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pandas as pd
import boto3
import mysql.connector
from mysql.connector import errorcode
import mysql.connector as msql
from mysql.connector import Error

db = mysql.connector.connect(user='admin', password='Pece2022', host='database-1.cdqlvhnbmps0.us-east-1.rds.amazonaws.com')
cursor = db.cursor()

create_database = "CREATE DATABASE IF NOT EXISTS analises"
cursor.execute(create_database)

conn = msql.connect(user='', password='', database='analises', host='')

try:
    empdata = pd.read_csv('s3://trusted-dados-instituicoes-financeiras/dim_instituicao.csv', index_col=False, delimiter = ';')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS instituicao;')
        cursor.execute("CREATE TABLE instituicao(CNPJ BIGINT not null primary key, instituicao varchar(255),tipo varchar(255))")
        for i,row in empdata.iterrows():
            sql = "INSERT INTO analises.instituicao VALUES (%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)


try:
    empdata = pd.read_csv('s3://trusted-dados-instituicoes-financeiras/dim_unidade.csv', index_col=False, delimiter = ';')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS unidade;')
        cursor.execute("CREATE TABLE unidade(CODIGO_UNIDADE BIGINT not null primary key, UNIDADE varchar(255))")
        for i,row in empdata.iterrows():
            sql = "INSERT INTO analises.unidade VALUES (%s,%s)"
            cursor.execute(sql, tuple(row))
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)

try:
    empdata = pd.read_csv('s3://trusted-dados-instituicoes-financeiras/dim_tempo.csv', index_col=False, delimiter = ';')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS tempo;')
        cursor.execute("CREATE TABLE tempo(ANO varchar(50), TRIMESTRE varchar(50), CODIGO_TEMPO int not null primary key)")
        for i,row in empdata.iterrows():
            sql = "INSERT INTO analises.tempo VALUES (%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)


try:
    empdata = pd.read_csv('s3://trusted-dados-instituicoes-financeiras/dim_serviceo.csv', index_col=False, delimiter = ';')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS servico;')
        cursor.execute("CREATE TABLE servico(CODIGO_SERVICO int not null primary key, TRIMESTRE varchar(500))")
        for i,row in empdata.iterrows():
            sql = "INSERT INTO analises.servico VALUES (%s,%s)"
            cursor.execute(sql, tuple(row))
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)
            
try:
    empdata = pd.read_csv('s3://trusted-dados-instituicoes-financeiras/fato_recl.csv', index_col=False, delimiter = ';')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS reclamacao;')
        cursor.execute("CREATE TABLE reclamacao(CODIGO_TEMPO int,	CNPJ BIGINT,INDICE float(10,2), QTD_RECLAMACOES_PROCEDENTES int,	QTD_RECLAMACOES_OUTRAS int,	QTD_RECLAMACOES_NAO_REGULADAS int,	QTD_RECLAMACOES int,	QTD_CLIENTES int,	QTD_CLIENTES_CCS int,	QTD_CLIENTES_SCR int)")
        for i,row in empdata.iterrows():
            sql = "INSERT INTO analises.reclamacao VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)

try:
    empdata = pd.read_csv('s3://trusted-dados-instituicoes-financeiras/fato_tarifas.csv', index_col=False, delimiter = ';')
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS tarifas;')
        cursor.execute("CREATE TABLE tarifas(CNPJ BIGINT,	CODIGO_SERVICO BIGINT,	CODIGO_UNIDADE BIGINT,	DATA_VIGENCIA varchar(50),	VALOR_MAXIMO float(10,2),	CODIGO_TIPO_VALOR int)")
        for i,row in empdata.iterrows():
            sql = "INSERT INTO analises.tarifas VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            conn.commit()
except Error as e:
            print("Error while connecting to MySQL", e)            

print("Importação concluída!")  

cursor.close()
conn.close()
db.close()
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
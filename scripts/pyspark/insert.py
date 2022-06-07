#!/usr/local/bin/python
# coding: latin-1
#extrac

##################################################################################################################################################################
# Created on 10 de Julho de 2021
#
#     Projeto base: ETL
#     Reposit�rio: S3
#     Linguagem: PYSPARK
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
#imports

from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max

# Cria objeto da Spark Session
spark = (SparkSession.builder.appName("DeltaExercise")
         .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate()
         )

# Importa o modulo das tabelas delta


# ler dados
assassinato = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .load("s3://datalake-tf/data/assassinatos.csv")

)

casamento = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .load("s3://datalake-tf/data/casamento.csv")

)

dc = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .load("s3://datalake-tf/data/dc.csv")

)

fifa = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .load("s3://datalake-tf/data/fifa.csv")

)
filmes = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .load("s3://datalake-tf/data/filmes.csv")

)

harrypotter = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .load("s3://datalake-tf/data/harrypotter.csv")

)

marvel = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .load("s3://datalake-tf/data/marvel.csv")

)

nba = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .load("s3://datalake-tf/data/nba.csv")

)

olimpiadas = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .load("s3://datalake-tf/data/olimpiadas.csv")

)

senhordosaneis = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .load("s3://datalake-tf/data/senhordosaneis.csv")

)

tarantino = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ",")
    .load("s3://datalake-tf/data/tarantino.csv")

)

# trasnformar dados

# assassinatos
assassinato = assassinato.select(
    'city', 'state', '2015_murders', 'change')

# casamento
casamento = casamento.drop("_c0")

# dc
dc = dc.drop("FIRST APPEARANCE")

# fifa
fifa = fifa.drop("tv_audience_share")

# filmes
filmes = filmes.drop("RottenTomatoes_Use")

# harrypotter
harrypotter = harrypotter.drop(
    "Id", "Blood status", "Hair colour", "Eye colour")

# marvel
marvel = marvel.drop("FIRST APPEARANCE")

# nba
nba = nba.drop("RK")

# olimpiadas
olimpiadas = olimpiadas.drop("ID")

# senhordosaneis
senhordosaneis = senhordosaneis.drop("Url")

# tarantino
tarantino = tarantino.drop("type")

#carregando arquivos delta on stage
(
    assassinato
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("city")
    .save("s3://datalake-tf/staging-zone/assassinato")
)

# casamento

(
    casamento
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("year")
    .save("s3://datalake-tf/staging-zone/casamento")
)

# dc

(
    dc
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("YEAR")
    .save("s3://datalake-tf/staging-zone/dc")
)

# fifa

(
    fifa
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("country")
    .save("s3://datalake-tf/staging-zone/fifa")
)

# filmes

(
    filmes
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("FILM")
    .save("s3://datalake-tf/staging-zone/filmes")
)

# harrypotter

(
    harrypotter
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("Name")
    .save("s3://datalake-tf/staging-zone/harrypotter")
)

# marvel

(
    marvel
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("YEAR")
    .save("s3://datalake-tf/staging-zone/marvel")
)

# nba

(
    nba
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("Player")
    .save("s3://datalake-tf/staging-zone/nba")
)

# olimpiadas

(
    olimpiadas
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("Year")
    .save("s3://datalake-tf/staging-zone/olimpiadas")
)

# senhordosaneis

(
    senhordosaneis
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("Name")
    .save("s3://datalake-tf/staging-zone/senhordosaneis")
)

# tarantino

(
    tarantino
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("movie")
    .save("s3://datalake-tf/staging-zone/tarantino")
)

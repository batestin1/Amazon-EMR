import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, lit, sum, asc

# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('datalake_enem_small_upsert')
logger.setLevel(logging.DEBUG)

# Definicao da Spark Session
spark = (SparkSession.builder.appName("DeltaExercise")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)


logger.info("Importing delta.tables...")
from delta import *


logger.info("Produzindo novos dados...")
assaNOVO = (
    spark.read.format("delta")
    .load("s3://datalake-tf/staging-zone/assassinato")
)

casaNOVO = (
    spark.read.format("delta")
    .load("s3://datalake-tf/staging-zone/casamento")
)

dcNOVO = (
    spark.read.format("delta")
    .load("s3://datalake-tf/staging-zone/dc")
)

fifaNOVO = (
    spark.read.format("delta")
    .load("s3://datalake-tf/staging-zone/fifa")
)

filmNOVO = (
    spark.read.format("delta")
    .load("s3://datalake-tf/staging-zone/filmes")
)

harryNOVO = (
    spark.read.format("delta")
    .load("s3://datalake-tf/staging-zone/harrypotter")
)

marvelNOVO = (
    spark.read.format("delta")
    .load("s3://datalake-tf/staging-zone/marvel")
)

nbaNOVO = (
    spark.read.format("delta")
    .load("s3://datalake-tf/staging-zone/nba")
)

olimpiadasNOVO = (
    spark.read.format("delta")
    .load("s3://datalake-tf/staging-zone/olimpiadas")
)

aneisNOVO = (
    spark.read.format("delta")
    .load("s3://datalake-tf/staging-zone/senhordosaneis")
)

tarantinoNOVO = (
    spark.read.format("delta")
    .load("s3://datalake-tf/staging-zone/tarantino")
)

#TRANSFORMANDO NOVOS DADOS
logger.info("PROCESSANDO A TABELA ASSASSINATO E eliminando duplicatas")
assaNOVO = assaNOVO.drop("change")

logger.info("PROCESSANDO A TABELA CASAMENTO E CRIANDO NOVO COLUNA")
casaNOVO = casaNOVO.drop("GD_4554")

logger.info("PROCESSANDO A TABELA DC COMICS E CRIANDO NOVO COLUNA CHAMADA EDITORA")
dcNOVO = dcNOVO.select(col("name").alias("nome"),
                          col("ID").alias("identidade"),
                          col("ALIGN").alias("carater"),
                          col("EYE").alias("olhos"),
                          col("HAIR").alias("cabelo"),
                          col("SEX").alias("genero"),
                          col("ALIVE").alias("situacao"),
                          col("Year").alias("ano"),
                          lit("dc_comics").alias("editora"))

logger.info("PROCESSANDO A TABELA FIFA E FILTRANDO APENAS A COMMEBOL")
fifaNOVO = fifaNOVO.filter("confederation == 'CONMEBOL'").select('country', 'population_share', 'gdp_weighted_share')

logger.info("PROCESSANDO A TABELA FILMES E AGRUPANDO POR IMDB")
filmNOVO = filmNOVO.groupBy('FILM').agg(sum('IMDB').alias('IMDB'),
                                         sum('IMDB_norm').alias('NORMAL_IMDB'),
                                         sum('IMDB_user_vote_count').alias('USUARIOS_IMDB')).sort(asc('FILM'))

logger.info("PROCESSANDO A TABELA HARRY POTTER COMICS E CRIANDO NOVO COLUNA CHAMADA INFORMAÇÃO")
harryNOVO = harryNOVO.select(col("Name").alias("nome"),
                          col("Gender").alias("genero"),
                          col("House").alias("casa"),
                          col("Wand").alias("varinha"),
                          col("Patronus").alias("patronus"),
                          col("Species").alias("raça"),
                          lit("book").alias("infos"))

logger.info("PROCESSANDO A TABELA DC COMICS E CRIANDO NOVO COLUNA CHAMADA EDITORA")
marvelNOVO = marvelNOVO.select(col("name").alias("nome"),
                          col("ID").alias("identidade"),
                          col("ALIGN").alias("carater"),
                          col("EYE").alias("olhos"),
                          col("HAIR").alias("cabelo"),
                          col("SEX").alias("genero"),
                          col("ALIVE").alias("situacao"),
                          col("Year").alias("ano"),
                          lit("dc_comics").alias("editora"))


logger.info("PROCESSANDO A TABELA NBA E CRIANDO AGRUPANDO POR MENORES REBOTS")
nbaNOVO  = nbaNOVO.groupBy("PLayer").agg(min(nbaNOVO['DRB%']).alias("MinRebotDef"),
                                        max(nbaNOVO['TS%']).alias("MaxPontosPart"))

logger.info("PROCESSANDO A TABELA OLIMPIADAS E RENOMEANDO COLUNAS")
olimpiadasNOVO = olimpiadasNOVO.select(col("Name").alias("nome"),
                          col("Sex").alias("genero"),
                          col("Age").alias("idade"),
                          col("Team").alias("Selecao"),
                          col("Year").alias("ano"),
                          col("City").alias("sede"),
                          col("Sport").alias("desporto"))

logger.info("PROCESSANDO A TABELA SENHOR DOS ANEIS E CRIANDO NOVO COLUNA CHAMADA INFO")
aneisNOVO = aneisNOVO.select(col("Name").alias("nome"),
                          col("Race").alias("raca"),
                          lit("livro").alias("info"))

logger.info("PROCESSANDO A TABELA TARANTINO E DROPANDO UMA COLUNA")
tarantinoNOVO = tarantinoNOVO.drop('minutes_in')


#assassinato
logger.info("INSERINDO A TABELA ASSASSINOS NO BUCKET OUTPUT")
(assaNOVO
.write
.mode("overwrite")
.format("delta")
.partitionBy("city")
.save("s3://datalake-tf/output/assassinato")
)


# casamento
logger.info("INSERINDO A TABELA CASAMENTO NO BUCKET OUTPUT")
(
    casaNOVO
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("year")
    .save("s3://datalake-tf/output/casamento")
)

logger.info("INSERINDO A TABELA DC COMICS NO BUCKET OUTPUT")
# dc

(
    dcNOVO
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("ano")
    .save("s3://datalake-tf/output/dc")
)
logger.info("INSERINDO A TABELA FIFA NO BUCKET OUTPUT")
# fifa

(
    fifaNOVO
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("country")
    .save("s3://datalake-tf/output/fifa")
)
logger.info("INSERINDO A TABELA FILMES NO BUCKET OUTPUT")
#filme
(
    filmNOVO 
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("USUARIOS_IMDB")
    .save("s3://datalake-tf/output/filmes")
)

logger.info("INSERINDO A TABELA HARRY POTTER NO BUCKET OUTPUT")
#harrypotter
(
    harryNOVO
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("casa")
    .save("s3://datalake-tf/output/harrypotter")
)

logger.info("INSERINDO A TABELA MARVEL COMICS NO BUCKET OUTPUT")
#marvel
(
    marvelNOVO
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("ano")
    .save("s3://datalake-tf/output/marvel")
)
logger.info("INSERINDO A TABELA NBA NO BUCKET OUTPUT")
#nba
(
    nbaNOVO
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("PLayer")
    .save("s3://datalake-tf/output/nba")
)
logger.info("INSERINDO A TABELA OLIMPIADAS NO BUCKET OUTPUT")
#olimpiadas
(
    olimpiadasNOVO
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("ano")
    .save("s3://datalake-tf/output/olimpiadas")
)
logger.info("INSERINDO A TABELA SENHOR DOS ANEIS NO BUCKET OUTPUT")
#senhor dos aneis
(
    aneisNOVO
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("nome")
    .save("s3://datalake-tf/output/senhordosaneis")
)
logger.info("INSERINDO A TABELA TARANTINO NO BUCKET OUTPUT")
#tarantino
(
    tarantinoNOVO
    .write
    .mode("overwrite")
    .format("delta")
    .partitionBy("movie")
    .save("s3://datalake-tf/output/tarantino")
)

logger.info("PROCESSO FINALIZADO COM SUCESSO!")
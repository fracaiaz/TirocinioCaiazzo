// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.functions._

//Qui mi costruisco il dataframe a partire dalla tabella che ho caricato tramite UI su databricks
//val collectDF = spark.read.format("avro").table("prova2")  

//Qui mi costruisco un Dataframe utilizzando la direttiva Load
val collectDF = spark.read.format("avro").load("/FileStore/tables/CO_0C279507D0567E9288E66C99FF467818-1.avro")
//Mostra l'intero DataFrame
//display(collectDF)

//Mostrami query_type e worker id
//collectDF.select("query_type","worker_id").show()


//Mostrami query_type e query_name che hanno un particolare ip
//collectDF.select("query_type","query_name").filter(collectDF("ip_prefix")==="104.28.16.0/20").show()


//Stampa schema ad albero
//collectDF.printSchema()


//prova query con linguaggio sql dove mostri query_type e name dove il ip_prefix non è nullo
collectDF.createOrReplaceTempView("tabSQL")
//val sqlDF = spark.sql("SELECT query_type,query_name FROM tabSQL WHERE ip_prefix IS NOT NULL  ")
//sqlDF.show()

//val sqlDF2 = spark.sql("SELECT query_type,query_name,ip_prefix FROM tabSQL WHERE query_name like '%AAAA'")
//sqlDF2.show()


val sqlDF3 = spark.sql("SELECT query_type,response_ttl FROM tabSQL WHERE response_ttl = (SELECT max(response_ttl) FROM tabSQL)")
sqlDF3.show()
//Mostrami il query_type, query_name e l'ip_prefix di tutti quei record in cui gli ip_prefix contengono 104
//collectDF.select("query_type","query_name","ip_prefix").filter($"ip_prefix".contains(104)).show()

//collectDF.select("query_name","query_type").filter(collectDF(collectDF.select(max("response_ttl")))).show()



// Databricks notebook source



//costruisco il DataFrame importandolo da una tabella che ho precedentemente caricato su Databricks tramite UI.
//val usersDF = spark.read.format("avro").option("header","true").option("sep",",").table("prova")  

//Qui mi costruisco un Dataframe utilizzando la direttiva Load
val usersDF = spark.read.format("avro").load("/FileStore/tables/CO_0C279507D0567E9288E66C99FF467818-1.avro")

usersDF.printSchema()


//Mi mostro le prime 10 righe delle colonne che mi interessano
usersDF.select("query_type","query_name","response_type","response_name","response_ttl","ip4_address").show(10)


//Conto quanti record di tipo query_type ci sono per le tipologia "AAAA" "A" "TXT"
usersDF.groupBy("query_type").count().filter(usersDF("query_type")=== "A" || usersDF("query_type")==="AAAA" || usersDF("query_type")==="TXT").show()



//Visualizzo prime 10 riche delle colonne query_type e response_ttl
usersDF.select("query_type","response_ttl").show(10)



//libreria per la media
import org.apache.spark.sql.functions._
//Provo se funziona la media su una singola colonna
usersDF.select(avg($"response_ttl"))



//Valore medio "response_ttl" per i record "A", "AAAA" e "TXT"
usersDF.groupBy("query_type").avg("response_ttl").filter(usersDF("query_type")=== "A" || usersDF("query_type")==="AAAA" || usersDF("query_type")==="TXT").show()



//Voglio  i primi 50 tipi di query che hanno un response_ttl inferiore ad un valore specifico
usersDF.select("query_type","response_ttl").filter(usersDF("response_ttl")<86400).show(50)


//Mi costruisco un secondo dataframe da un altro documento
//val tab2= spark.read.format("avro").option("header","true").option("sep",",").table("prova2")



//Mostrami i valori di rttl per le query txt
val provaDF = usersDF.select("query_type","response_ttl").filter(usersDF("query_type")==="SOA")
provaDF.show(1000)




usersDF.select(max("response_ttl")).show()
//Mostrami per ogni tipo di query il più alto valore di response_ttl
usersDF.groupBy("query_type").max("response_ttl").show()


//Mostra solo il tipo di query che si ripete almeno 10000 volte
usersDF.groupBy("query_type").count().filter("count > 100000").show()



//Mostrami il tipo di query e il suo rttl che ha il response ttl più alto di tutti
usersDF.select("query_type").filter("response_ttl >200000").show()



//Mostra tutte le statistiche della tabella (avg,massimo,minimo)
//usersDF.describe().show()







// COMMAND ----------

 

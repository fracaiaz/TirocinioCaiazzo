// Databricks notebook source
import spark.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{stddev_samp, stddev_pop}

//Qui mi costruisco un Dataframe utilizzando la direttiva Load
val usersDF = spark.read.format("avro").load("/FileStore/tables/CO_0C279507D0567E9288E66C99FF467818-1.avro")

//Mostro tutta la tabella caricata
//display(usersDF)


//Mostro lo schema delle dipendenze
//usersDF.printSchema()

//Trova media dei timestamps
usersDF.select(avg("timestamp")).coalesce(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/df/FunzStat.csv")


//Trova media dei timestamps di ogni query_type
usersDF.groupBy("query_type").avg("timestamp").coalesce(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/df/FunzStat.csv")


//Fai la somma di dei response_ttl di tutti i record con query_type uguale ad A
usersDF.groupBy("query_type").sum("response_ttl").filter(usersDF("query_type")==="A").coalesce(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/df/FunzStat.csv")

//Fai la somma di tutti i valori di response_ttl
usersDF.select(sum("response_ttl")).coalesce(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/df/FunzStat.csv")

//Trova il valore massimo per ogni tipo di query.
usersDF.groupBy("query_type").max("response_ttl").coalesce(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/df/FunzStat.csv")

//Trova il valore massimo in assoluto e mostra il query_type
usersDF.createOrReplaceTempView("tabSQL")
val sqlDF3 = spark.sql("SELECT query_type,response_ttl FROM tabSQL WHERE response_ttl = (SELECT max(response_ttl) FROM tabSQL)")
sqlDF3.coalesce(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/df/FunzStat.csv")


//Trova media dei rtt_response di  MS,NS,A
usersDF.groupBy("query_type").avg("response_ttl").filter(usersDF("query_type")==="MS" || usersDF("query_type")==="NS" || usersDF("query_type")==="A").coalesce(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/df/FunzStat.csv")

//Deviazione standard in base al query_type
usersDF.createOrReplaceTempView("df")
sqlContext.sql("""SELECT query_type, stddev(response_ttl)
                     FROM df
                     GROUP BY query_type""").coalesce(1).write.mode(SaveMode.Append).format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/df/FunzStat.csv")

//Varianza in base al query_type
/*usersDF.createOrReplaceTempView("df")
sqlContext.sql("""SELECT query_type, variance(response_ttl)
                     FROM df
                     GROUP BY query_type""").coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header","true").save("dbfs:/FileStore/df/FunzStat.csv")

*/

//Varianza colonna response_ttL
//sqlContext.sql("SELECT variance(response_ttl) FROM df").show()


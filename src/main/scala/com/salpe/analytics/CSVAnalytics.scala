package com.salpe.analytics

import java.net.URL

import org.apache.spark.sql.SparkSession
import java.nio.file.Paths
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

object CSVAnalytics {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")

    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().appName("com.salpe.analytics").getOrCreate();
    val url: URL = getClass.getResource(".")
    val resourcePath = Paths.get(url.toURI)
    val sqlContext = new SQLContext(sc)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(resourcePath.resolve("hotels.csv").toString)

    df.show

  
    df.filter(df
      .col("uri")
      .rlike("https?:\\/\\/(www\\.)?[-a-zA-Z0-9@:%._\\+~#=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%_\\+.~#?&//=]*)")).filter(df.col("stars").rlike("[1-5]")).
      sort(df.col("stars").desc,df.col("name").asc).repartition(1).write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", "true").save(resourcePath.resolve("result").toString);

    sc.stop

  }
}
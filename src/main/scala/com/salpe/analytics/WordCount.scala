package com.salpe.analytics

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.nio.file.Paths
import java.net.URL
import java.nio.file.Files
import java.io.File

object WordCount {
  
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")

    val sc = new SparkContext(conf)

    val url: URL = getClass.getResource(".")

    val resourcePath = Paths.get(url.toURI)

    val textFile = sc.textFile(resourcePath.resolve("sample.txt").toString)

    Files.deleteIfExists(resourcePath.resolve("result"))

    val out: File = resourcePath.resolve("result").toFile;
    if (out.exists)
      delete(out)

    print(resourcePath.resolve("result").toString)

    textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => (a + b)).saveAsTextFile(resourcePath.resolve("result").toString)

    sc.stop();
  }

  def delete(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
    file.delete
  }
}
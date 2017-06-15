package com.strokova.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class WordCountService(sc: SparkContext) {

  def printWordCount(fileName: String): Unit = (init _ andThen wordCount andThen print).apply(fileName)

  private def init(path: String): RDD[String] = {
    sc.textFile(path)
  }

  private def wordCount(rdd: RDD[String]): RDD[(String, Int)] = {
    rdd
      .flatMap(_.split(Array(' ', ',', '.')))
      .filter(_.length > 2)
      .map((_, 1))
      .reduceByKey(_ + _)
  }

  private def print(rdd: RDD[(String, Int)]): Unit = {
    rdd.sortBy(_._2, ascending = false).foreach(println)
  }

}
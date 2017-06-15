package com.strokova.spark

import org.apache.spark.{SparkConf, SparkContext}

object Main {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("spark-simple-app")
    val context = new SparkContext(conf)
    context.setLogLevel("OFF")
    val pathFile = getClass.getClassLoader.getResource("text.txt").getPath
    new WordCountService(context).printWordCount(pathFile)
  }

}

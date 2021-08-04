package com.sparkPractice
import org.apache.spark.sql.SparkSession

object testFile {

  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir" , "C:/hadoop")

    val spark = SparkSession.builder().master("local[1]").appName("partitionDemo").getOrCreate()

    println("trying to read the file .... ")
    val rddTest = spark.sparkContext.textFile("file:///SparkProject/rdd.txt")
    rddTest.collect().foreach(line => println(line))
    println("file has been loaded successfully")

    rddTest.saveAsTextFile("file:///SparkProject/newRDD")
    println("FIle has been written to the disc successfully")

  }
}

package com.sparkPractice

import org.apache.spark.sql.SparkSession

object firstDemo {

  val spark = SparkSession.builder().master("local[1]").appName("demoSpark").getOrCreate()

  def main(args: Array[String]): Unit = {


    val data = Seq("Uttam", "hello")


    print(data)
    println("Spark job has been completed successfully")
  }


}

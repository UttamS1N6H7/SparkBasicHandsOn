package com.sparkPractice
import org.apache.spark.sql.SparkSession

object partitionTry {


  def main(args: Array[String]): Unit = {



    System.setProperty("hadoop.home.dir" , "C:/hadoop")

    val spark = SparkSession.builder().master("local[*]").appName("partitionDemo").getOrCreate()
    import spark.sqlContext.implicits._

    val rdd1 = spark.sparkContext.parallelize(Seq("Hello there","Hello how are you","are you alright","are you okay"))
    val rdd2 = rdd1.flatMap(line => line.split(" ")).map(line => (line , 1)).reduceByKey(_+_)
    val newDF1 = rdd2.toDF("Name" , "Id")

    rdd2.collect().foreach(a => println(a))
    newDF1.show()
    //newDF1.write.mode("Overwrite").csv("file:///SparkProject/TestFile/DataFrame")
    rdd2.coalesce(1).saveAsTextFile("C:/SparkProject/TestFile/RDD")
  }
}

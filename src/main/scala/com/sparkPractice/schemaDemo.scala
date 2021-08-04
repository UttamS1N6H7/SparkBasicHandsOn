package com.sparkPractice
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object schemaDemo {

   val spark = SparkSession.builder().master("local[1]").appName("schemaDemo").getOrCreate()

  def main(args: Array[String]): Unit = {

    var stud_rdd = spark.sparkContext.parallelize(Seq(
      Row(101, "Uttam" , 88.23),
      Row(102, "Ram" , 99.21),
      Row(103, "Shyam", 89.20)
    ))

    var schema_list = List(("id" , "Int"),("name","String"),("percentage","Double"))
    var schema = new StructType()
    schema_list.map(line => schema = schema.add(line._1 , line._2))
    val students = spark.createDataFrame(stud_rdd , schema)
    students.show()

    students.filter(col("Name") === "Ram").show()
    students.write.format("csv").save("file:///SparkProject/OutputFiles/schemaDF")

  }


}

package com.sparkPractice

import org.apache.spark.sql.SparkSession

object mySqlDemo {
 val spark = SparkSession.builder().master("local[1]").appName("sql connection").getOrCreate()

  def main(args: Array[String]): Unit = {

    val sqlQuery = """select a.actor_id, a.first_name, a.last_name from actor a, actor_info b where a.actor_id = b.actor_id and a.actor_id < 20"""
    val dataframe_mysql = spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sakila")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", s"( $sqlQuery) t")
      .option("user", "root")
      .option("password", "uttam123")
      .load()

    dataframe_mysql.show()
    dataframe_mysql.printSchema()

   dataframe_mysql.write.format("jdbc")
      .mode("overwrite")
      .option("url", "jdbc:mysql://localhost:3306/sakila")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "sakila.testDemo")
      .option("user", "root")
      .option("password", "uttam123")
      .save()

  }

}

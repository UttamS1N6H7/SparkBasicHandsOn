package com.sparkPractice
import java.io.File
import java.nio.file.{ Files, Path, StandardCopyOption }
import org.apache.spark.sql.SparkSession

object fileChecks {
  val spark = SparkSession.builder().master("local[1]").appName("fileChecking").getOrCreate()

  def main(args: Array[String]): Unit = {
    val path = "C:\\SparkProject\\InputFiles\\Billing"
    val files = new File(path).list.toList // gives list of file names including extensions in the path `path`

    println(files)

    for (f <- files){
      var account_num = f.substring(13,16)
      println(account_num)
    }
  }
}

package com.sparkPractice
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{to_date, to_timestamp}
import org.apache.spark.sql.types.IntegerType

object salesData {

  val spark = SparkSession.builder().master("local[1]").appName("salesTrend").getOrCreate()

  def main(args: Array[String]): Unit = {

    val salesDF = spark.read.format("csv")
      .option("header", "true")
      .load("file:///SparkProject/InputFiles/Sales/SalesJan2009.csv")

    //type(salesDF)

    val modifiedDF = salesDF.withColumn("Transaction_Date", to_date(col("Transaction_Date"), "MM/dd/yyyy"))
      .withColumn("US Zip", col("US Zip").cast(IntegerType))

    val castedDF = modifiedDF.selectExpr("cast(Transaction_date as date) Transaction_date",
      "Product",
      "cast(Price as Int) Price",
      "Payment_Type",
      "Name",
      "City",
      "State",
      "Country",
      "cast(Account_Created as date) Account_Created",
      "cast(Last_Login as date) Last_Login",
      "Latitude",
      "Longitude",
      "`US Zip`"
    )

    //salesDF.printSchema()
    //salesDF.take(10).foreach(line => println(line))

    //castedDF.printSchema()
    val paymentSpec = castedDF.filter(col("Payment_Type") === "Visa")
    paymentSpec.write.format("csv").save("file:///SparkProject/OutputFiles/sales/paymentDF")

    //paymentSpec.show()


  }

}

package com.sparkPractice
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, input_file_name, udf, when, regexp_extract, current_timestamp, expr, lit}
import org.apache.spark.sql.types.{DataType, DateType, DecimalType, IntegerType, StringType, StructField, StructType, TimestampType}

import java.io.File

object fileGBBU {

  val spark = SparkSession.builder().master("local[1]").appName("GBBU upload").getOrCreate()
  val working_dir = "C:\\SparkProject\\InputFiles\\Atlantis\\"

  def main(args: Array[String]): Unit = {

//    val path = "C:\\SparkProject\\InputFiles\\Billing"
    val files = new File(working_dir).list.toList // gives list of file names including extensions in the path `path`

    println(files)

    for (f <- files){
      val readBBU = spark.read.format("csv")
        .option("header", "false")
        .option("delimiter" , "|")
        .option("ignoreLeadingWhiteSpace",false)
        .option("ignoreTrailingWhiteSpace",false)
        .load(working_dir + "/" + f.toString)


      //val invoiceId: Column = col(f.toString)
      //val working_dir_length: Int = working_dir.length + 1

      //readBBU.show(truncate = false)
      //readBBU.printSchema()
      //println(readBBU.columns.size)
      //if ()
      if(readBBU.columns.size == 72)
        {
          println("It's GBBU")
          if(col("_c0") === "ADJUSTMENTS")
            callGBBUFunction(readBBU)
          else
            println("There is no Adjustments records")
        }
        else
        println("It's CBBU")

    }

  }

  def callGBBUFunction(inputDF: DataFrame) = {

    import org.apache.spark.sql.functions.udf

    val filterGBBU = inputDF.filter(col("_c0") === "ADJUSTMENTS")
    filterGBBU.show(truncate = false)
    filterGBBU.printSchema()

    /**val schemaAdj = Seq(("AdjustementCategory", "TariffName", "AdjustementSubCategory",
      "ChargeDescription", "ReasonText", "AdjustementDate", "EndDate", "addressLine1",
      "zipCode", "CSSJobNumber", "CustomerOrderNumber1", "CustomerOrderNumber2", "Quantity",
      "Units", "UnitRate", "AdjustmentAmount", "TaxIdentifierForVAT", "CSSAccountNumber",
      "ProductType", "ORServiceId", "CircuitNumber", "MDFsite", "RoomPSI", "ServiceId",
      "EventClassName", "EventName", "CBUKReferenceNumber", "EventSourceId", "MACCode",
      "FreeText", "TRCStartDate", "ClearCode", "TRCDescriptionCode", "PriceLineId", "PriceLineDescription")) **/

    val schemaAdj = new StructType(
      Array(
        StructField("RecordType", StringType, false),
        StructField("AdjustementCategory", StringType, false),
        StructField("TariffName", StringType, true),
        StructField("AdjustementSubCategory", StringType, true),
        StructField("ChargeDescription", StringType, true),
        StructField("ReasonText", StringType, true),
        StructField("AdjustementDate",StringType, true),
        StructField("EndDate", StringType, true),
        StructField("addressLine1", StringType, true),
        StructField("zipCode", StringType, true),
        StructField("CSSJobNumber", StringType, true),
        StructField("CustomerOrderNumber1",StringType, true),
        StructField("CustomerOrderNumber2", StringType, true),
        StructField("Quantity", StringType, true),
        StructField("Units", StringType, true),
        StructField("UnitRate", StringType, true),
        StructField("AdjustmentAmount", StringType, true),
        StructField("TaxIdentifierForVAT", StringType, true),
        StructField("CSSAccountNumber", StringType, true),
        StructField("ProductType", StringType, true),
        StructField("ORServiceId", StringType, true),
        StructField("CircuitNumber", StringType, true),
        StructField("MDFsite", StringType, true),
        StructField("RoomPSI", StringType, true),
        StructField("ServiceId", StringType, true),
        StructField("EventClassName", StringType, true),
        StructField("EventName", StringType, true),
        StructField("CBUKReferenceNumber", StringType, true),
        StructField("EventSourceId", StringType, true),
        StructField("MACCode", StringType, true),
        StructField("FreeText", StringType, true),
        StructField("TRCStartDate", StringType, true),
        StructField("ClearCode", StringType, true),
        StructField("TRCDescriptionCode", StringType, true),
        StructField("PriceLineId", StringType, true),
        StructField("PriceLineDescription", StringType, true)
      )
    )

    //val get_inv_num = udf{filePath: String => filePath.split("\\.")(4)}
    spark.udf.register("get_only_file_name", (fullPath: String) => fullPath.split("/").last)
    //UDF to get the file name
    //val get_only_file_name = udf((fullPath: String) => fullPath.split("/").last)

    //val timestamp = datetime.datetime.fromtimestamp(time.time()).strtime("%Y-%m-%d %H:%M:%S")
    val newColToAdd = List(("InvoiceId", expr("get_only_file_name(input_file_name())")),
      ("InvoiceVersion",expr("get_only_file_name(input_file_name())")),
      ("BillingAccountNumber",expr("get_only_file_name(input_file_name())")),
      ("InvoiceProductionDate",expr("current_timestamp()")),
      ("InvoiceScheduledDate",expr("current_timestamp()")),
      ("InvoiceStatus", lit("created"))
    )

    val rddTrans = filterGBBU.rdd
    val renamedDF = spark.sqlContext.createDataFrame(rddTrans , schemaAdj)
    //val addColDF = renamedDF.withColumn("InvoiceId", get_only_file_name(input_file_name()).substr(1,14))
    //addColDF.show(truncate = false)
    val addedColDF = newColToAdd.foldLeft(renamedDF){
      (tempdf, cols) => tempdf.withColumn(cols._1, cols._2)

    }

    val selectDF = addedColDF.select("RecordType","InvoiceId","InvoiceVersion","BillingAccountNumber","InvoiceProductionDate",
      "InvoiceScheduledDate","InvoiceStatus","AdjustementCategory","TariffName",
      "AdjustementSubCategory","ChargeDescription","ReasonText","AdjustementDate",
      "EndDate","addressLine1","zipCode","CSSJobNumber","CustomerOrderNumber1",
      "CustomerOrderNumber2","Quantity","Units","UnitRate","AdjustmentAmount",
      "TaxIdentifierForVAT","CSSAccountNumber","ProductType","ORServiceId",
      "CircuitNumber","MDFsite","RoomPSI","ServiceId","EventClassName",
      "EventName","CBUKReferenceNumber","EventSourceId","MACCode","FreeText",
      "TRCStartDate","ClearCode","TRCDescriptionCode","PriceLineId","PriceLineDescription")
    //addColDF.write.format("csv").mode("append").save("C:\\SparkProject\\OutputFiles\\GBBU_csv")
    selectDF.show()
  }

}



import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.{date_format, to_timestamp, udf}
import Utils.{ConnectToS3, readDroneMessageCsv}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}



object SparkAnalyze  {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkAnalyseRoadViolations")
      .getOrCreate()

    val accessKey = "AKIAS7AOU2S4KEKI3Q7K"
    val secretAccessKey = "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"

    ConnectToS3(sparkSession = spark, AWSKey = accessKey, AWSSecretKey = secretAccessKey)


    val schemaMessage = StructType(Seq(
      StructField("droneId", StringType, nullable = true),
      StructField("violationId", StringType, nullable = true),
      StructField("imageId", StringType, nullable = true),
      StructField("violationCode", IntegerType, nullable = true),
      StructField("date", StringType, nullable = true),
      StructField("latitude", DoubleType, nullable = true),
      StructField("longitude", DoubleType, nullable = true)))


    val s3DronesMsgPath = "s3a://drones-message-via-firehose/*/*/*/*/*"
    val s3NYPDMsgPath = "s3a://nypd-messages-via-firehose/*/*/*/*/*"

    val dfRegMessages = readDroneMessageCsv(s3DronesMsgPath, spark, schemaMessage)
    dfRegMessages.printSchema()
    val count1 = dfRegMessages.count()
    println("TOTAL DRONES ROWS: " + count1)

    val dfNYPDMessages = readDroneMessageCsv(s3NYPDMsgPath, spark, schemaMessage)
      dfNYPDMessages.cache()
      val count = dfNYPDMessages.count()
      println("TOTAL NYPD MESSAGE ROWS: " + count)




    dfNYPDMessages.withColumn("date",
      to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("week_day_number", date_format(col("date"), "u"))
      .withColumn("week_day_abb", date_format(col("date"), "E"))
      .show()




  }

}

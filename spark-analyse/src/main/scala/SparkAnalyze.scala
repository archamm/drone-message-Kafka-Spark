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

    val accessKey = "AKIAS7AOU2S4LWDP4VVB"
    val secretAccessKey = "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"

    ConnectToS3(sparkSession = spark, AWSKey = accessKey, AWSSecretKey = secretAccessKey)


    val schemaMessage = StructType(Seq(
      StructField("droneId", StringType, nullable = true),
      StructField("violationId", StringType, nullable = true),
      StructField("date", StringType, nullable = true),
      StructField("latitude", DoubleType, nullable = true),
      StructField("longitude", DoubleType, nullable = true)))


    val s3RegPath = "s3a://drones-messages/regular-messages/"
    val dfRegMessages = readDroneMessageCsv("/Users/matthieuarchambault/Documents/Epita/SCIA-COURS/Scala - SPARK/scala_prestacops/NYPDCsvToS3/DEST/reg", spark, schemaMessage)
    dfRegMessages.printSchema()
    dfRegMessages.show()
    dfRegMessages.printSchema()

    val schemaMessageViolation = StructType(
      StructField("violationId", StringType, nullable = true) ::
        StructField("imageId", StringType, nullable = true) ::
        StructField("violationCode", IntegerType, nullable = true) :: Nil)

    val s3VioPath = "s3a://drones-messages/violation-messages/"
    val dfViolationMessages = readDroneMessageCsv("/Users/matthieuarchambault/Documents/Epita/SCIA-COURS/Scala - SPARK/scala_prestacops/NYPDCsvToS3/DEST/vio", spark, schemaMessageViolation)
    dfViolationMessages.show()
    dfViolationMessages.printSchema()

    val joinedViolationDf = dfRegMessages.join(
      dfViolationMessages,
      Seq("violationId"),
    "left")
    joinedViolationDf.show()
    joinedViolationDf.printSchema()


    joinedViolationDf.withColumn("date",
      to_timestamp(col("date"), "dd/MM/yyyy'T'HH:mm:ss"))
      .withColumn("week_day_number", date_format(col("date"), "u"))
      .withColumn("week_day_abb", date_format(col("date"), "E"))
      .show()




  }

}

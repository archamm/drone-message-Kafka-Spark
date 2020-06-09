
import org.apache.spark.sql.SparkSession
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




    dfRegMessages.filter("violationCode IS NOT null").
      groupBy("violationCode").
      count()
      .orderBy("violationCode")
      .show()

    dfRegMessages.withColumn("date",
      to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("week_day_number", date_format(col("date"), "u"))
      .withColumn("Day of the week of violation", date_format(col("date"), "E"))
      .filter("violationId IS NOT NULL")
      .groupBy("Day of the week of violation")
      .count()
      .orderBy("count")
      .na.drop()
      .show()

    val checkIfWeekEnd: Int => String = (arg: Int) => {if (arg < 6) "Week day" else "Week-end day"}
    val checkIfWeekEndUdf = udf(checkIfWeekEnd)


    dfNYPDMessages.withColumn("date",
      to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("week_day_number", date_format(col("date"), "u"))
      .withColumn("Type of day", checkIfWeekEndUdf(col("week_day_number")))
      .groupBy("Type of day")
      .count()
      .show()
    dfRegMessages.withColumn("date",
      to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("week_day_number", date_format(col("date"), "u"))
      .withColumn("Type of day", checkIfWeekEndUdf(col("week_day_number")))
      .groupBy("Type of day")
      .count()
      .show()



    dfRegMessages.withColumn("date",
      to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("Hour of Violation", date_format(col("date"), "HH"))
      .filter("violationId IS NOT NULL").groupBy("Hour of Violation")
      .count()
      .orderBy("Hour of Violation")
      .na.drop()
      .show()

    val checkIfDayTime: Int => String = (arg: Int) => {if (arg > 6 && arg < 21) "Day Time" else "NightTime"}
    val checkIfDayTimeUdf = udf(checkIfDayTime)


    dfNYPDMessages.withColumn("date",
      to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("Hour of Violation", date_format(col("date"), "HH"))
      .withColumn("Type of Hour", checkIfDayTimeUdf(col("Hour of Violation")))
      .groupBy("Type of Hour")
      .count()
      .show()
    dfRegMessages.withColumn("date",
      to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss"))
      .withColumn("Hour of Violation", date_format(col("date"), "HH"))
      .withColumn("Type of Hour", checkIfDayTimeUdf(col("Hour of Violation")))
      .groupBy("Type of Hour")
      .count()
      .show()

  }

}

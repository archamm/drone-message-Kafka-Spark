import java.time.LocalDateTime

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import S3Connect.ConnectToS3

object Utils extends java.io.Serializable {
  val getDayOfInfraction: String => String = LocalDateTime.parse(_).getDayOfWeek.toString
}
object SparkAnalyze  {


  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkAnalyseRoadViolations")
      .getOrCreate()

    ConnectToS3(sparkSession = spark, AWSKey = "***", AWSSecretKey = "***")

    val dfRegMessages = spark.read.options(Map("inferSchema"->"true","delimiter"->";","header"->"true"))
      .csv("s3a://drones-messages/drones-messages.csv").dropDuplicates()

    dfRegMessages.show()
    dfRegMessages.printSchema()
    val dfViolationMessages = spark.read.options(Map("inferSchema"->"true","delimiter"->";","header"->"true"))
      .csv("s3a://drones-messages/drones-violations-messages.csv").dropDuplicates()
    dfViolationMessages.show()
    dfViolationMessages.printSchema()

    val joinedViolationDf = dfRegMessages.join(
      dfViolationMessages,
      Seq("violationId"),
    "left")
    joinedViolationDf.show()
    joinedViolationDf.printSchema()


    val getDayOfInfractionUDF: UserDefinedFunction = udf(Utils.getDayOfInfraction)

    val dayOfInfractionDf = joinedViolationDf.withColumn("dayOfInfraction", getDayOfInfractionUDF(col("date")))
    dayOfInfractionDf.filter("violationId IS NOT NULL").groupBy("dayOfInfraction").count().orderBy("count").show()
    dayOfInfractionDf.groupBy("violationMessage").count().orderBy("count").show()


  }


}

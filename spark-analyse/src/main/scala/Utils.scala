
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Utils {

  def readDroneMessageCsv(path:String, sparkSession: SparkSession, schema: StructType):DataFrame = {
    sparkSession.read.options(Map("delimiter"->";")).schema(schema)
      .csv(path).dropDuplicates()
  }
  def ConnectToS3(sparkSession: SparkSession, AWSKey: String, AWSSecretKey:String): Unit ={
    sparkSession.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", AWSKey)
    // Replace Key with your AWS secret key (You can find this on IAM
    sparkSession.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", AWSSecretKey)
    sparkSession.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  }
}

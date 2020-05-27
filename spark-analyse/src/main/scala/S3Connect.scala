import java.time.LocalDateTime

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf

object S3Connect {
  def ConnectToS3(sparkSession: SparkSession, AWSKey: String, AWSSecretKey:String): Unit ={
    sparkSession.sparkContext
      .hadoopConfiguration.set("fs.s3a.access.key", "AKIAS7AOU2S4LWDP4VVB")
    // Replace Key with your AWS secret key (You can find this on IAM
    sparkSession.sparkContext
      .hadoopConfiguration.set("fs.s3a.secret.key", "d4jC/2g6McJaqz+XUaxbY7YXfWrbIkn3v6PooAtO")
    sparkSession.sparkContext
      .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")

  }
}

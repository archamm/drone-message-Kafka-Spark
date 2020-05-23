import java.io.{File, FileReader, PrintWriter}
import java.time.Duration
import java.util.{Collections, Properties}
import java.io.FileOutputStream
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.auth.BasicAWSCredentials
import org.apache.commons.io.FileUtils

object DroneConsumer {

  case class DroneMessage(DroneId: Int, message: String)

  def main(args: Array[String]): Unit = {
    val topicName = "mqtt.drones-messages"

    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "something")
    val consumer = new KafkaConsumer[String, JsonNode](props)
    //val MAPPER = new ObjectMapper
    consumer.subscribe(Collections.singletonList(topicName))
    while (true) {
      println("Polling")
      val records = consumer.poll(Duration.ofSeconds(1))
      records.forEach(rec => {
        println(s"Consumed record with key ${rec.key()} and message ${formatDroneMessage(rec.value().toString)}")
        writeInS3(rec.timestamp(), formatDroneMessage(rec.value().toString))
        Thread.sleep(700)
      })
    }
    consumer.close()

  }
  def buildProperties(configFileName: String): Properties = {
    val properties = new Properties()
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "scala_example_group")
    properties.load(new FileReader(configFileName))
    properties
  }
  def writeInS3(key: Long, messsage: String) :Unit = {


    val bucketName = "drones-messages"          // specifying bucket name

    //file to upload
    val file = new File("drones-messages.csv")
    /* These Keys would be available to you in  "Security Credentials" of
        your Amazon S3 account */
    val AWS_ACCESS_KEY = "******"
    val AWS_SECRET_KEY = "******"
    val provider = new AWSStaticCredentialsProvider(
      new BasicAWSCredentials(AWS_ACCESS_KEY,AWS_SECRET_KEY)
    )

    val amazonS3Client = AmazonS3ClientBuilder
      .standard
      .withCredentials(provider)
      .withRegion("eu-west-3") // or whatever  your region is
      .build

    // This will create a bucket for storage
    val o = amazonS3Client.getObject(bucketName, "drones-messages.csv")
    val s3is = o.getObjectContent
    val fos = new FileOutputStream(file, true)
    val read_buf = new Array[Byte](1024)
    var read_len = 0
    while ( {read_len = s3is.read(read_buf) ; read_len } > 0)
      fos.write(read_buf, 0, read_len)
    s3is.close()
    fos.write(messsage.getBytes)
    fos.write("\n".getBytes)

    amazonS3Client.putObject(bucketName, "drones-messages.csv", file)
    FileUtils.deleteQuietly(file)
  }
  def formatDroneMessage(message:String): String = {
    val formattedMessage :String = message.substring(message.indexOf(">") + 2)
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val droneMessage = objectMapper.readValue(formattedMessage, classOf[DroneMessage])
    droneMessage.DroneId.toString + " ; " + droneMessage.message

  }
}
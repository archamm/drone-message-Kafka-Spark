import java.nio.ByteBuffer
import java.time.format.DateTimeFormatter

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.kinesisfirehose.model.{PutRecordRequest, Record}
import com.amazonaws.services.kinesisfirehose.{AmazonKinesisFirehose, AmazonKinesisFirehoseClient}
import com.amazonaws.services.s3.AmazonS3ClientBuilder

import scala.annotation.tailrec
import scala.io.Source


object NypdCsvToS3 {

  def writeFilesToS3(folderPath: String, bucketName: String, AWS_ACCESS_KEY: String
                     , AWS_SECRET_KEY: String, pathInBucket: String): Unit = {
    Utils.getListOfFiles(folderPath).foreach(file => {

      val provider = new AWSStaticCredentialsProvider(
        new BasicAWSCredentials(AWS_ACCESS_KEY,AWS_SECRET_KEY)
      )
      val amazonS3Client = AmazonS3ClientBuilder
        .standard
        .withCredentials(provider)
        .withRegion("eu-west-3")
        .build
      amazonS3Client.putObject(bucketName, pathInBucket + file.getName, file)

    })
  }

  def parseLine(line:String): Map[String, String] = {
    val regex = """"((?:[^"\\]|\\[\\"ntbrf])+)"""".r
    val data = regex.replaceAllIn(line, "").split(',')
    println(data)
    if (data.length < 19) {
       null
    }
    else {
      println(line)
      val coords =
        if (Utils.precinctIdToCoordinatesMap.contains(data(14).toInt))
          Utils.precinctIdToCoordinatesMap(data(14).toInt)
        else
          Utils.Point(40.7205994, -74.0083416)

      println(data(19))
      println(parseTime(data(19)))
      println(coords)
      Map(
        "violationCode" -> data(5),
        "violationId" -> data(0),
        "latitude" -> coords.lat.toString,
        "longitude" -> coords.long.toString,
        "date" -> (toSimpleDate(data(4)) + "T" + parseTime(data(19))))
    }
  }
  def checkTimeFormat(time:String): Boolean ={
    time.length == 5 &&  time(0).isDigit &&  time(1).isDigit &&  time(2).isDigit &&  time(3).isDigit
  }

  def toSimpleDate(dateString: String): String = {
    val parser = DateTimeFormatter.ofPattern("MM/dd/yyyy")
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
    formatter.format(parser.parse(dateString))


  }


  def parseTime(time:String): String ={
    println(time)
    if (!checkTimeFormat(time)) {
      println("format didn't work")
      "00:00:00"
    } else if((time(0).toString + time(1).toString).toInt == 12){
      if (time.contains("P")){
        time(0).toString + time(1).toString + ":" + time(2).toString + time(3).toString + ":00"
      }
      else
        (((time(0).toString + time(1).toString).toInt - 12)).toString + ":" + time(2).toString + time(3).toString + ":00"

    } else if (time.contains("A")){
      time(0).toString + time(1).toString + ":" + time(2).toString + time(3).toString + ":00"
    }
    else
      (((time(0).toString + time(1).toString).toInt + 12) % 24).toString + ":" + time(2).toString + time(3).toString + ":00"
  }

  def sendLine(lineMap: Map[String, String], firehoseClient: AmazonKinesisFirehose): Unit ={
    println(lineMap)
    if (lineMap != null) {
      val message = ";" + lineMap("violationId") + ";;" + lineMap("violationCode") + ";" +
        lineMap("date") + ";" + lineMap("latitude") + ";" + lineMap("longitude") + "\n"
      val fireHoseDeliveryStreamName = "nypd-message-flux"
      val putRecordRequest = new PutRecordRequest()
      putRecordRequest.setDeliveryStreamName(fireHoseDeliveryStreamName)

      val record = new Record()
        .withData(ByteBuffer.wrap(message.getBytes))
      putRecordRequest.setRecord(record)
      val putRecordResult = firehoseClient.putRecord(putRecordRequest)
      println(putRecordResult.toString)

    }
  }
  @tailrec def UntilNLine[A](numberOfLine: Int, body: => A): Unit = {
    if (numberOfLine > 0) {
      body
      UntilNLine(numberOfLine = numberOfLine - 1, body)
    }
  }


    @tailrec def writeFlow(ite: Iterator[String], firehoseClient: AmazonKinesisFirehose): Unit = {
      val list = if (ite.hasNext) ite.next() else Nil
      if (list != Nil) {
        sendLine(parseLine(list.toString),firehoseClient)
        writeFlow(ite, firehoseClient)
      }
    }




  def main(args: Array[String]): Unit = {


    val pathToNypdCsvFiley: String = "/Users/matthieuarchambault/Downloads/NYPDParkingViolations/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv"

    val AWS_ACCESS_KEY = "AKIAS7AOU2S4KEKI3Q7K"
    val AWS_SECRET_KEY = "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"
    val firehoseClient = AmazonKinesisFirehoseClient.builder.withRegion("eu-west-3")
      .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(AWS_ACCESS_KEY,AWS_SECRET_KEY))).build

    val file = Source.fromFile(pathToNypdCsvFiley)
    val ite = file.getLines()
    ite.next()
    writeFlow(ite, firehoseClient)
    file.close()





  }


}


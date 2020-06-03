import java.io.{BufferedWriter, FileWriter}
import java.time.format.DateTimeFormatter

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3ClientBuilder}

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
        if (Utils.precinctIdToCoordinatesMap.get(data(14).toInt).isDefined)
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
    val formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy")
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

  def writeLine(lineMap: Map[String, String], fwReg: BufferedWriter, fwVio: BufferedWriter): Unit ={
    println(lineMap)
    if (lineMap != null) {
      fwReg.write(";" + lineMap("violationId") + ";" + lineMap("date") + ";" + lineMap("latitude") + ";" + lineMap("longitude") + "\n")
      fwVio.write(lineMap("violationId") + ";;" + lineMap("violationCode") + "\n")
    }
  }
  @tailrec def UntilNLine[A](numberOfLine: Int, body: => A): Unit = {
    if (numberOfLine > 0) {
      body
      UntilNLine(numberOfLine = numberOfLine - 1, body)
    }
  }


  @tailrec def writeFlow(numberOfLine: Int, ite: Iterator[String], destFolder: String): Unit = {
    //open new file
    val fwReg = new BufferedWriter(new FileWriter(destFolder + "reg/nypd-csv-data-reg-" + java.util.UUID.randomUUID.toString + ".csv", true))
    val fwVio = new BufferedWriter(new FileWriter(destFolder + "vio/nypd-csv-data-vio-" + java.util.UUID.randomUUID.toString + ".csv", true))
    writeFlow_rec(numberOfLine, ite)
    @tailrec def writeFlow_rec(numberOfLine: Int, ite: Iterator[String]): Unit = {
      val list = if (ite.hasNext) ite.next() else Nil
      if (numberOfLine > 0 && list != Nil) {
        writeLine(parseLine(list.toString), fwReg, fwVio)
        writeFlow_rec(numberOfLine = numberOfLine - 1, ite)
      }
    }
    fwReg.close()
    fwVio.close()
    if (ite.hasNext) {
      writeFlow(numberOfLine = numberOfLine, ite, destFolder)
    }
  }




  def main(args: Array[String]): Unit = {

    val destFolder = "/Users/matthieuarchambault/Documents/Epita/SCIA-COURS/Scala - SPARK/scala_prestacops/NYPDCsvToS3/DEST/"

    val NUMBER_OF_LINES = 200000


    val pathToNypdCsvFiley: String = "/Users/matthieuarchambault/Downloads/NYPDParkingViolations/Parking_Violations_Issued_-_Fiscal_Year_2014__August_2013___June_2014_.csv"


    val file = Source.fromFile(pathToNypdCsvFiley)
    val ite = file.getLines()
    ite.next()
    writeFlow(NUMBER_OF_LINES, ite, destFolder)
    file.close()
    val accessKey = "AKIAS7AOU2S4LWDP4VVB"
    val secretAccessKey = "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"

    writeFilesToS3(destFolder + "reg", "drones-messages", accessKey, secretAccessKey, "regular-messages/")
    writeFilesToS3(destFolder + "vio", "drones-messages", accessKey, secretAccessKey, "violation-messages/")



  }


}


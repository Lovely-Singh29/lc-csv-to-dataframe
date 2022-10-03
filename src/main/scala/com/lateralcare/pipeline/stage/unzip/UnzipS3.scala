package com.lateralcare.pipeline.stage.unzip

import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ObjectListing, PutObjectRequest}
import com.lateralcare.pipeline.common.{S3AccessClient, StageFailedArgument, StageFailedException}
import com.lateralcare.pipeline.stage.entrypoint.`trait`.PipelineStage
import com.lateralcare.pipeline.stage.entrypoint.model.StageArgument
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream

import java.io.ByteArrayInputStream
import scala.collection.JavaConverters._

object UnzipS3 extends PipelineStage {

  var amazonS3Client: AmazonS3Client = null;
  val BUCKET_NAME = S3AccessClient.bucketName

 private def processFiles(filePath : String, destPath : String) = {


     val randomNum = new scala.util.Random

    var zis: GzipCompressorInputStream = null
    try {
      System.out.println("UnzipS3 Placing File into S3 -FileName-->" + filePath)
      // download file and read line by line
      val obj = amazonS3Client.getObject(BUCKET_NAME, filePath)

      val buffer = new Array[Byte](1024)
      //zip file content
      zis = new GzipCompressorInputStream((obj.getObjectContent()));

      //get the zipped file list entry
      var ze = zis.read(buffer);

      while (ze >= 0) {

        var fileName = zis.getMetaData().getFilename()
        System.out.println("file fileName : " + fileName);
        if (fileName != null) {
          //validate whether the file with same name exists already

          val objMetaData = amazonS3Client.getObjectMetadata(BUCKET_NAME, destPath + fileName)

          if (objMetaData.getContentType() != null && fileName != null) {
            // Appending random numbers at the end of file name if already exists
            val splitVal = fileName.split('.')

            if (splitVal.length > 1) {
              fileName = splitVal(0) + randomNum.nextInt() + "." + splitVal(1)
            }
          }
        }
        var len: Int = zis.read(buffer);

        while (len > 0) {
          len = zis.read(buffer)
        }

        val inputStream = new ByteArrayInputStream(buffer)
        val putObj = new PutObjectRequest(BUCKET_NAME, destPath + fileName, inputStream, null) //BUCKET_NAME, FILE_NAME, buffer)

        amazonS3Client.putObject(putObj)
        // amazonS3Client.deleteObject(BUCKET_NAME, FILE_NAME)
        ze = zis.read(buffer);

        System.out.println("UnzipS3 Placing File into S3 -Completed --File name-->" + filePath)

      }

    } catch {
       case ex : Exception => throw new Exception(ex.getMessage)
    }
    finally {
      zis.close()
    }
  }
  private def nextBatch(listing: ObjectListing, keys: List[String] = Nil): List[String] = {

    val pageKeys = listing.getObjectSummaries.asScala.map(_.getKey).toList

    if (listing.isTruncated)
      nextBatch(amazonS3Client.listNextBatchOfObjects(listing), pageKeys ::: keys)
    else
      pageKeys ::: keys
  }

  def unzipS3(sourcePathRef:String,destPath:String): Unit = {

     try {

      amazonS3Client = S3AccessClient.getS3Client

      val fileList: ObjectListing = amazonS3Client.listObjects(BUCKET_NAME, sourcePathRef)


      var keys: List[String] = nextBatch(fileList)
      println("keys " + keys)
      keys = keys.filter(key => key.endsWith(".gz"))
      println("keys post filter" + keys)
      System.out.println("UnzipS3 -- Before looping through list of files got from S3")

      keys.map(filePath => {
         processFiles(sourcePathRef, destPath)
      })


    } catch {
       case ex : Exception => throw new Exception(ex.getMessage)
    }
  }

  override var sourcePath: String = ""
  override var destinationPath: String = ""
  override def getStageName() = {
    "Unzip"
  }

  override def getStageId() = {
    "123"
  }

  override def Run(stageArgument: StageArgument) = {

    sourcePath = stageArgument.getSourcePath()
    destinationPath = stageArgument.getDestinationPath()
    try {
      System.out.println("UnzipS3 Process started")
      UnzipS3.unzipS3(sourcePath, destinationPath)
      System.out.println("UnzipS3 Process Completed")
    }
    catch {
      case ex : Exception => {
        val stageFailedArgument = new StageFailedArgument()
        stageFailedArgument.setStageName(getStageName())
        stageFailedArgument.setErrorMessage(ex.getMessage)
        stageFailedArgument.setSourcePath(sourcePath)
        stageFailedArgument.setDestinationPath(destinationPath)
        stageFailedArgument.setStackTrace(ex.getStackTrace)

        throw new StageFailedException(stageFailedArgument)

        }
    }
    true
  }
}

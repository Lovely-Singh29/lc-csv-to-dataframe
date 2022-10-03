package com.lateralcare.pipeline.movefiles

import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{CopyObjectRequest,DeleteObjectRequest, ObjectListing}
import scala.collection.JavaConverters._
import com.lateralcare.pipeline.common.S3AccessClient
import com.lateralcare.pipeline.stage.entrypoint.model.StageArgument

object MoveFiles extends App{


  // Accessing AmazonClient
  var amazonS3Client: AmazonS3Client = null;
  // Listing file names in batches
  private def nextBatch(listing: ObjectListing, keys: List[String] = Nil): List[String] = {

    val pageKeys = listing.getObjectSummaries.asScala.map(_.getKey).toList

    if (listing.isTruncated)
      nextBatch(amazonS3Client.listNextBatchOfObjects(listing), pageKeys ::: keys)
    else
      pageKeys ::: keys
  }

  //  Passing StageArgument as an argument inside run method with s3 BucketName, SourcePath & DestPath
  def Run(stageArgument: StageArgument): Boolean = {

    val BUCKET_NAME = S3AccessClient.bucketName
    val suffix = stageArgument.getFilter().getExpression()

    val NEGATE = stageArgument.getFilter().getNegate()

    // Listing files based on specific filtered file type or if not negate the other type of file expect the
    //filtered file type
    try {

      amazonS3Client = S3AccessClient.getS3Client
      val fileList: ObjectListing = amazonS3Client.listObjects(BUCKET_NAME, "GZIP File") // getting file names
      var keys: List[String] = nextBatch(fileList) // saving file names in keys
      println("keys " + keys)

      if (NEGATE == false) {
        keys = keys.filter(key => key.endsWith(suffix)) // filter specific file type
      }
      else if (NEGATE) {
        keys = keys.filter(key => !key.endsWith(suffix)) // filter other files
      }

      println("keys post filter" + keys)

      System.out.println("GZIP File -- Before looping through list of files got from S3")

      // split file name
      keys.map(filePath => {

        val fileNames = filePath.split('/')

        val fileName = fileNames.last

        System.out.println("filename : " + fileName)

        // copy files from s3 and place it in the destination folder
        val copyFiles = new CopyObjectRequest(BUCKET_NAME, filePath, BUCKET_NAME, stageArgument.getDestinationPath() + fileName)

        amazonS3Client.copyObject(copyFiles)

        System.out.println("text files Placing Files into S3 -Completed --File name-->" + filePath)

        // delete existing files from source after placing in destination folder
        val deleteFiles = new DeleteObjectRequest(BUCKET_NAME, filePath)

        amazonS3Client.deleteObject(deleteFiles)

      })
      true
    }
    catch {
      case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString)
        false
      case ace: AmazonClientException => System.err.println("Exception: " + ace.toString)
        false
      case dsad: Exception => System.err.println("Exception: " + dsad.toString)
        false
    }
  }
}
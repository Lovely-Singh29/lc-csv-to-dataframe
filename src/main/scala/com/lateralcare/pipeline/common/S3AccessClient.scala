package com.lateralcare.pipeline.common

import com.amazonaws.{AmazonClientException, AmazonServiceException}
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.lateralcare.pipeline.config.AppConfiguration
import scala.util.Properties

object S3AccessClient {


  var amazonS3Client : AmazonS3Client= null

  // -- Reads and Holds the configuration
  var appConfig = new AppConfiguration()
  val bucketName = appConfig.getS3Configuration().getString("bucketName")
  val awsAccessKey = appConfig.getS3Configuration().getString("accessKey")
  val awsSecretKey = appConfig.getS3Configuration().getString("secretKey")

  def getS3Client : AmazonS3Client = {

    val AWS_ACCESS_KEY =  Properties.envOrElse( "AWS_ACCESS_KEY", awsAccessKey)
    val AWS_SECRET_KEY = Properties.envOrElse("AWS_SECRET_KEY", awsSecretKey)

    try {
      val awsCredentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
      amazonS3Client = new AmazonS3Client(awsCredentials)


    } catch {
      case ase: AmazonServiceException => System.err.println("Exception: " + ase.toString)
      case ace: AmazonClientException => System.err.println("Exception: " + ace.toString)
    }


    amazonS3Client
  }
}

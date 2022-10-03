package com.lateralcare.pipeline.common


import com.lateralcare.pipeline.config.AppConfiguration
import scala.util.Properties
import com.amazonaws.{ClientConfiguration, Protocol}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.model.{ SendMessageRequest}
import com.amazonaws.services.sqs.{AmazonSQS,  AmazonSQSClientBuilder}

object ExceptionManager  {



  var appConfig = new AppConfiguration()
  val bucketName = appConfig.getS3Configuration().getString("bucketName")
  val awsAccessKey = appConfig.getS3Configuration().getString("accessKey")
  val awsSecretKey = appConfig.getS3Configuration().getString("secretKey")
  val region = appConfig.getS3Configuration().getString("region")
  val QUEUE_NAME = appConfig.getS3Configuration().getString("queueName")



  def sendMessage(message : String) : Unit = {

    println("Entering sendMessage")
  try
  {

    val AWS_ACCESS_KEY = Properties.envOrElse("AWS_ACCESS_KEY", awsAccessKey)
    val AWS_SECRET_KEY = Properties.envOrElse("AWS_SECRET_KEY", awsSecretKey)

    val credentials = new BasicAWSCredentials(AWS_ACCESS_KEY, AWS_SECRET_KEY)
    val credentialsProvider = new AWSStaticCredentialsProvider(credentials)

    var regions = Regions.valueOf(region)

    var clientConfiguration = new ClientConfiguration()
    clientConfiguration.setProtocol(Protocol.HTTP)

    val sqsClient: AmazonSQS = AmazonSQSClientBuilder.standard()
      .withCredentials(credentialsProvider)
      .withClientConfiguration(clientConfiguration)
      .withRegion(regions)
      .build()

    val queueUrl = sqsClient.getQueueUrl(QUEUE_NAME).getQueueUrl()

    println("queueURL " + queueUrl)

    val sendMessageRequest = new SendMessageRequest().withQueueUrl(queueUrl).withMessageBody(message)

    sqsClient.sendMessage(sendMessageRequest)

    println("Message sent to SQS queue Successfully")

  }
    catch {
      case e: Exception => println("exception caught while sending message to SQS queue : " + e.printStackTrace())
    }


  }


}

package com.lateralcare.pipeline.stage.entrypoint

import com.lateralcare.pipeline.common.{ExceptionManager, StageFailedException, Utility}
import com.lateralcare.pipeline.config.AppConfiguration
import com.lateralcare.pipeline.stage.validation.Validator

import scala.util.Failure

object DataPipeline extends App {

  // -- Set default path of the input file
  var filePath = ""

  // -- holds the client's map id
  var clientMapID = ""

  // -- Reads and Holds the configuration
  var appConfig = new AppConfiguration()

  try {

    // -- Get the file path as an application argument
    if (args.length > 0) {
      filePath = s"${args(0)}"
    }

    // -- Get the ClientID from the file path
    clientMapID = Utility.getClientIDFromFilePath(filePath)

    // -- Invoke the validation step
    Validator.main(Array(filePath))

  }
  catch {
    case stageFailedException: StageFailedException => {
      println("Exception in DataPipeline errorMessage: "+stageFailedException.getMessage)
      Failure(stageFailedException)
      // Sending failed error message to AWS SQS queue
      try {
        ExceptionManager.sendMessage(stageFailedException.getMessage)
      }
      catch {
        case unknown: Exception => {
          println("Error while sending error message to Queue --> "+unknown.getMessage)
          Failure(unknown)
        }
      }
    }
    case unknown: Exception => {
      println(s"Exception: $unknown")
      Failure(unknown)
    }
  }
}

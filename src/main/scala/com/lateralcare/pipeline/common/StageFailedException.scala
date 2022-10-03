package com.lateralcare.pipeline.common

import org.apache.spark.status.api.v1.StackTrace

class StageFailedException(stageFailedArgument: StageFailedArgument) extends Exception{

  override def getMessage: String = {

    var customMessage = new StringBuilder("Stage Name : "+ stageFailedArgument.getStageName())
    customMessage ++= "==> Source Path : "+stageFailedArgument.getSourcePath()
    customMessage ++= "==> Destination Path : "+stageFailedArgument.getDestinationPath()
    customMessage ++= "==> Error Message : "+stageFailedArgument.getErrorMessage()
    customMessage ++= "==> Stack Trace : "+getStackTrace.toString

    customMessage.toString()
  }

}

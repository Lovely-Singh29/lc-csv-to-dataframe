package com.lateralcare.pipeline.stage.validation

import com.lateralcare.pipeline.common.{StageFailedArgument, StageFailedException}
import com.lateralcare.pipeline.stage.entrypoint.`trait`.PipelineStage
import com.lateralcare.pipeline.stage.entrypoint.model.StageArgument
import com.lateralcare.pipeline.stage.transformation.Transformer
import com.lateralcare.pipeline.stage.validation.factory.FilePropertyValidatorFactory
import com.lateralcare.pipeline.stage.validation.model.DataQualityManager

import scala.util.Failure

object Validator extends PipelineStage {

  private def validate(filePath: String) = {

    try {

      // -- Should not proceed if param is not given
      if (filePath.isEmpty()) false

      val dataQualityManager = new DataQualityManager()

      if (!dataQualityManager.validate(filePath)) {
        // -- File data quality is met, so lets not proceed
        // -- Move the file to unprocessed queue
        // -- ToDo: Call the Exception Manager to Move the file to unprocessed queue
        false
      }

      // -- Proceed with next stage in the pipeline

    }
    catch {
      case unknown: Exception => {
        println(s"Exception: $unknown")
        Failure(unknown)
        throw new Exception(unknown.getMessage)
      }
    }
  }


  override var sourcePath = ""
  override var destinationPath = ""

  override def getStageName() = {
    "Validator"
  }

  override def getStageId() = {
    "123"
  }

  override def Run(stageArgument: StageArgument) = {

    sourcePath = stageArgument.getSourcePath
    destinationPath = stageArgument.getDestinationPath

    try{
      validate(sourcePath)
    }
    catch {
      case ex: Exception => {
        var stageFailedArgument = new StageFailedArgument()
        stageFailedArgument.setStageName(getStageName())
        stageFailedArgument.setErrorMessage(ex.getMessage)
        stageFailedArgument.setStackTrace(ex.getStackTrace)
        stageFailedArgument.setSourcePath(sourcePath)
        stageFailedArgument.setDestinationPath(destinationPath)

        throw new StageFailedException(stageFailedArgument)
      }
    }
    true
  }
}
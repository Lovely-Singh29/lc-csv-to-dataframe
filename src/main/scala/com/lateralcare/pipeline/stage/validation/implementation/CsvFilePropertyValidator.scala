package com.lateralcare.pipeline.stage.validation.implementation

import com.lateralcare.pipeline.common.{Constants, StageFailedArgument, StageFailedException, Utility}
import com.lateralcare.pipeline.stage.validation.`trait`.FilePropertyValidator

import scala.util.Failure

class CsvFilePropertyValidator extends FilePropertyValidator{

  override def CheckFileFormat(filePath: String): Boolean = {
    try {
      if( Utility.getFileExtension(filePath) == Constants.CSV_EXTENSION ) true else false
    }
    catch {
      case unknown: Exception => {
        println(s"Exception: $unknown")
        Failure(unknown)
        var stageFailedArgument = new StageFailedArgument()
        stageFailedArgument.setStageName("CsvFilePropertyValidator")
        stageFailedArgument.setErrorMessage(unknown.getMessage)
        stageFailedArgument.setStackTrace(unknown.getStackTrace)
        stageFailedArgument.setSourcePath(filePath)

        throw new StageFailedException(stageFailedArgument)
        false
      }
    }
  }
}
package com.lateralcare.pipeline.stage.validation.factory

import com.lateralcare.pipeline.common.{Constants, StageFailedArgument, StageFailedException, Utility}
import com.lateralcare.pipeline.stage.validation.`trait`.FilePropertyValidator
import com.lateralcare.pipeline.stage.validation.implementation.{CsvFilePropertyValidator, ParquetFilePropertyValidator}

object FilePropertyValidatorFactory {

  def apply(filePath: String): FilePropertyValidator = {

    // -- Get the extension to create appropriate object
    val fileType = Utility.getFileExtension(filePath)

    fileType match {
      case "csv" => new CsvFilePropertyValidator()
      case "parquet" => new ParquetFilePropertyValidator()
      case _ => {
        var stageFailedArgument = new StageFailedArgument()
        stageFailedArgument.setStageName("FilePropertyValidatorFact")
        stageFailedArgument.setErrorMessage("Invalid Type")
        stageFailedArgument.setSourcePath(filePath)

        throw new StageFailedException(stageFailedArgument)
      }
    }
  }
}

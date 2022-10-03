package com.lateralcare.pipeline.stage.transformation.factory

import com.lateralcare.pipeline.common.{Constants, StageFailedArgument, StageFailedException, Utility}
import com.lateralcare.pipeline.stage.transformation.`trait`.LCParser
import com.lateralcare.pipeline.stage.transformation.implementation.{CSVParser, ParquetParser}

object LCDataParserFactory {

  def apply(filePath: String): LCParser = {

    // -- Get the extension to create appropriate object
    val fileType = Utility.getFileExtension(filePath)

    fileType match {
      case "csv" => new CSVParser()
      case "parquet" => new ParquetParser()
      case _ => {
        var stageFailedArgument = new StageFailedArgument()
        stageFailedArgument.setStageName("LCDataParserFactory")
        stageFailedArgument.setErrorMessage("Invalid Type")
        stageFailedArgument.setSourcePath(filePath)

        throw new StageFailedException(stageFailedArgument)
      }

    }
  }

}

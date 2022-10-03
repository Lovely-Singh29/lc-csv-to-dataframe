package com.lateralcare.pipeline.stage.transformation

import com.lateralcare.pipeline.common.{StageFailedArgument, StageFailedException, Utility}
import com.lateralcare.pipeline.config.AppConfiguration
import com.lateralcare.pipeline.stage.entrypoint.`trait`.PipelineStage
import com.lateralcare.pipeline.stage.entrypoint.model.StageArgument
import com.lateralcare.pipeline.stage.transformation.factory.LCDataParserFactory
import com.lateralcare.pipeline.stage.transformation.model.DataManipulator
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Failure

object Transformer extends PipelineStage {

  val appConfig = new AppConfiguration()

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName(appConfig.getSpark_AppName())
    .config("spark.mongodb.input.uri", appConfig.getMongo_Mapping_Uri())
    .getOrCreate()

  private def transform( sourcePath : String) = {

        var filePath = sourcePath

        try {

          val clientMapID = Utility.getClientIDFromFilePath(filePath)
          val parser = LCDataParserFactory.apply(filePath)

          parser.setfilepath(filePath)
          parser.setsparkSession(spark)

          val parsedDataFrame = parser.parse()

          // -- Manipulate the destination data by renaming, ordering, filtering
          val destinationDataFrame = new DataManipulator(clientMapID, spark, parsedDataFrame).formatData()

          if (destinationDataFrame != null) {

            // -- ToDo: Need to write this in to MongoDB using DataAccessManager
            // -- Write it as a parquet file
            destinationDataFrame.write.mode(SaveMode.Overwrite).parquet(appConfig.getParquetDestFile())
          }

        }
        catch {
          case unknown: Exception => {
            // -- ToDo:Use the LogManager to log the exception
            println(s"Exception: $unknown")
            val stageFailedArgument = new StageFailedArgument()
            stageFailedArgument.setStageName(getStageName())
            stageFailedArgument.setErrorMessage(unknown.getMessage)
            stageFailedArgument.setStackTrace(unknown.getStackTrace)
            stageFailedArgument.setSourcePath(sourcePath)
            stageFailedArgument.setDestinationPath(destinationPath)

            throw new StageFailedException(stageFailedArgument)

            Failure(unknown)
          }
        }
  }

  override var sourcePath = ""
  override var destinationPath = ""

  override def getStageName(): String = {
    "Transform"
  }

  override def getStageId() = {
    "123"
  }

  override def Run(stageArgument: StageArgument) = {
    sourcePath = stageArgument.getSourcePath()
     transform(sourcePath)
    true
  }
}



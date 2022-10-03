package com.lateralcare.pipeline.stage.enrichlookup

import com.lateralcare.pipeline.common.{LCConstants, StageFailedArgument, StageFailedException}
import com.lateralcare.pipeline.config.AppConfiguration
import com.lateralcare.pipeline.stage.entrypoint.`trait`.PipelineStage
import com.lateralcare.pipeline.stage.entrypoint.model.StageArgument
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/*
This Object is to Enrich a given file
by adding data from the look up file
 */
object EnrichLookUp extends PipelineStage{

  // -- Variables to hold file path
  override var sourcePath = ""
  override var destinationPath = ""
  var lookupFilePath = ""

  // -- To hold application properties
  val appConfig = new AppConfiguration()

  // -- To hold the spark session
  var sparkSession: SparkSession = null

  // -- ToDo: Uncomment the below function to test the stage locally and comment before check-in to github
//  startStageWithStageSpec()

  /*
  This method will be called by the framework
  to verify the Stage name

  Params: None

  Return: Stage name string
   */
  override def getStageName(): String  = {
    "EnrichLookUp".toLowerCase()
  }

  /*
  This method will be called by the framework
  to verify the Stage id

  Params: None

  Return: Stage ID string
   */
  override def getStageId(): String = {
  "STG006" // -- Hardcoded as of now, needs to be fetched from database
  }

  /*
  Entry point to the stage which will be called by
  the framework to perform the stage's functionality

  Params:
  stageArgument: StageArgument - stage argument that will be sent by framework

  Return: Returns true or false based on the completion of the lookup
   */
  override def Run(stageArgument: StageArgument): Boolean = {

    try {

      // -- Create the spark session which will be
      // -- used to load the given file to dataframe
      sparkSession = SparkSession.builder()
        .master("local[*]")
        .appName(appConfig.getSpark_AppName())
        .getOrCreate()

      // -- get the lookupspec which has the details on source,
      // -- lookup and destination files from the stage argument
      val lookupSpec = stageArgument.getlookupspec()

      // -- Build the source, lookup and destination path
      sourcePath = stageArgument.getSourcePath() + lookupSpec.sourcefile.filename
      destinationPath = stageArgument.getDestinationPath() + lookupSpec.destinationfile.filename
      lookupFilePath = stageArgument.getLookupFilePath() + lookupSpec.lookupfile.filename

      // -- Load lookup file to dataframe after validating the supported format
      val lookupDataFrame: DataFrame = validateAndLoadFile(lookupFilePath,
        lookupSpec.lookupfile.fileformat,
        sparkSession)

      // -- Load source file to dataframe after validating the supported format
      var sourceDataFrame: DataFrame = validateAndLoadFile(sourcePath,
        lookupSpec.sourcefile.fileformat,
        sparkSession)

      // -- Select the required fields on the source based on the configuration
      sourceDataFrame = sourceDataFrame.select(lookupSpec.query.select)

      // -- Join the dataframes based on the join condition given in the configuration
      var joinedDataFrame: DataFrame = sourceDataFrame.join(lookupDataFrame,
        sourceDataFrame(lookupSpec.query.join.joincondition.righttablecolumn) ===
          lookupDataFrame(lookupSpec.query.join.joincondition.lefttablecolumn),
        lookupSpec.query.join.jointype)

      // -- Validate te required file format before writing into destination path
      validateFile(lookupSpec.destinationfile.fileformat)

      // -- Check the resultant dataset of the join
      // -- Save the original source data if there were issues in the join
      if(joinedDataFrame == null){
        joinedDataFrame = sourceDataFrame
      }

      // -- Write the resultant dataframe to the given parquet file
      joinedDataFrame.write.mode(SaveMode.Overwrite).parquet(destinationPath)
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

  /*
  Validates the file format that given in the configuration and throws Exception

  Params:
  fileFormat: String - file format that comes from the lookupspec

  Return: Returns true or false based on the format that supported
   */
  private def validateFile(fileFormat: String): Boolean = {
    if (fileFormat != LCConstants.PARQUET_EXTENSION) {
      throw new Exception("Format not supported! File should be in parquet format")
    }
    true
  }

  /*
  Validates the file format that given in the configuration and throws Exception

  Params:
  fileToLoad: String - file that needs to be loaded to a dataframe which configured in the lookupspec
  fileFormat: String - file format that comes from the lookupspec
  sparkSession: SparkSession - Spark session that will be used to load given file

  Return: Returns the dataframe after loading the given file
   */
  private def validateAndLoadFile(fileToLoad: String, fileFormat: String, sparkSession: SparkSession ): DataFrame ={

    validateFile(fileFormat)

    // -- Load the given lookup file (support only parquet as per requirement)
    sparkSession.sqlContext.read.format(LCConstants.PARQUET_EXTENSION)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(fileToLoad)
  }

  /*
  This function will be used to locally test the stage using the
  "src/main/resources/enrichlookupspec.json" file which has
  the stagespec. Reads the above file and uses the json create
  the stageArgument and passes the same to Run method.

  Params:
  sparkSession: SparkSession - Spark session that will be used to load given file

  Return: Return the status to the stage execution
   */
  private def startStageWithStageSpec(): Boolean = {

    // -- Create the spark session which will be
    // -- used to load the given file to dataframe
    sparkSession = SparkSession.builder()
      .master("local[*]")
      .appName(appConfig.getSpark_AppName())
      .getOrCreate()

    var sampleJsonPath: String = "src/main/resources/enrichlookupspec.json"

    // -- Read the above sample json file which has the stagespec
    val sampleDataFrame = sparkSession.read
      .option("inferSchema", "true")
      .option("multiline", "true")
      .json(sampleJsonPath)

    // -- When you collect json from DataFrame, it collects that as list
    // -- In our case the sample file has only one item,so get the first item
    val stringJson = sampleDataFrame.toJSON.collectAsList.get(0)

    // -- Call the Run method with the StageArgument
    Run(new StageArgument(stringJson))
  }

}

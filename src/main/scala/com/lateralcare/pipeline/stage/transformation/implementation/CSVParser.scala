package com.lateralcare.pipeline.stage.transformation.implementation

import com.lateralcare.pipeline.common.LCConstants
import com.lateralcare.pipeline.stage.transformation.`trait`.LCParser
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
This class parses the given csvfile and store the data in a DataFrame
 */
class CSVParser extends LCParser {
  var filePath: String = ""
  var sparkSession: SparkSession = null

  override def setfilepath(filePathParasm: String): Unit = {
    filePath = filePathParasm
  }

  override def setsparkSession(sparkSessionParam: SparkSession): Unit = {
    sparkSession = sparkSessionParam
  }

  /*
  This method parses the given csv file
  return: DataFrame
   */
  override def parse(): DataFrame = {
    // -- Add the CSV content to DataFrame for further use
    sparkSession.sqlContext.read.format(LCConstants.CSV_EXTENSION)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)
  }
}

package com.lateralcare.pipeline.stage.transformation.implementation

import com.lateralcare.pipeline.common.LCConstants
import com.lateralcare.pipeline.stage.transformation.`trait`.LCParser
import org.apache.spark.sql.{DataFrame, SparkSession}

/*
This class parses the given csvfile and store the data in a DataFrame
 */
class ParquetParser extends LCParser {
  var filePath: String = " "
  var sparkSession: SparkSession = null

  override def setfilepath(filepath: String): Unit = {
    filePath= filepath
  }

  override def setsparkSession(spark: SparkSession): Unit = {
    sparkSession=spark
  }
  /*
  This method parses the given csv file
  return: DataFrame
   */
  override def parse(): DataFrame = {
    // -- Add the Parquet content to DataFrame for further use
    sparkSession.sqlContext.read.format(LCConstants.PARQUET_EXTENSION)
      .option("header", "true")
      .option("inferSchema", "true")
      .load(filePath)
  }
}

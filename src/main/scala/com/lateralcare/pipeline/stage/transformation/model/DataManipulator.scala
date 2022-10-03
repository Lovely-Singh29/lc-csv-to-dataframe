package com.lateralcare.pipeline.stage.transformation.model

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

/*
This class manipulates the given DataFrame by renaming, ordering, filtering
 */
class DataManipulator(mapid: String, sparkSession: SparkSession, parsedData: DataFrame) {

  // -- Stores the mongo spark connector
  private val mapCollectionDF = MongoSpark.load(sparkSession)

  // -- Stores the source and Alias fields
  private var mappingDF: DataFrame = null

  // -- Contains the parsed data
  private val parsedDataFrame: DataFrame = parsedData

  def formatData(): DataFrame = {
    // -- Load the mapping collection
    loadSourceAndDestMap()

    // -- Filter out unwanted columns
    var destinationDataFrame = filterColumns(parsedDataFrame)

    // -- Rename the columns
    destinationDataFrame = renameColumns(destinationDataFrame)
    destinationDataFrame
  }

  // -- Private functions

  /*
  This function loads the source and destination fields which will be used to map and transform
  returns: DataFrane that has Source and Alias fields
   */
  private def loadSourceAndDestMap(): DataFrame = {
    // -- Get the mapping document from MongoDB for the given clientmapid
    val mappingDocumentDF = mapCollectionDF.filter(mapCollectionDF("MapID") === mapid)
    mappingDF = mappingDocumentDF.withColumn("tmp", explode(col("Mapping")))
      .select("tmp.Source", "tmp.Alias")
    mappingDF
  }

  /*
  This function rename the columns with the destination columns name
  Param: sourceDF: DataFrame - Source dataframe to be renamed
  return: Modified Dataframe which has the renamed columns
   */
  private def renameColumns(sourceDF: DataFrame): DataFrame = {
    sourceDF.columns.foldLeft(sourceDF) { (memoDF, colName) =>
      memoDF.withColumnRenamed(colName, getDestColumnName(colName))
    }
  }

  /*
  This function filters the columns with the selected columns
  Param: sourceDF: DataFrame - Source dataframe to be filtered
  return: Modified Dataframe which has the filtered columns
   */
  private def filterColumns( sourceDF: DataFrame): DataFrame = {
    val columnNames = mappingDF.select("Source").map(_.getString(0))(Encoders.STRING).collect().toList
    sourceDF.select(columnNames.map(name => col(name)): _*)
  }

  /*
  This function helps to lookup and get the destination
  column name with the given column string

  Param: sourccColumnName: String - String to be looked up
  return: Modified Dataframe which has the filtered columns
   */
  private def getDestColumnName(sourccColumnName: String): String = {
    val names = mappingDF.filter(mappingDF("Source").equalTo(sourccColumnName)).select("Alias").collectAsList()
    names.get(0).getString(0)
  }
}

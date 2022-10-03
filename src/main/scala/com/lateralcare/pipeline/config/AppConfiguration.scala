package com.lateralcare.pipeline.config

import com.typesafe.config.{Config, ConfigFactory}

/*
This class parses the application.conf file and exposes methods to access them
 */
class AppConfiguration {

  // -- Holds the whole configuration file details
  val config = ConfigFactory.load("application.conf")

  /*
  This method gives the MongoDB's connection URI taken from the config file
  return: String value of the URi
   */
  def getMongo_Mapping_Uri(): String = {
    getMongo_Mapping().getString("url")
  }

  /*
  This method gives the MongoDB's connection URI taken from the config file
  return: String value of the URI
   */
  def getSpark_AppName(): String = {
    getSpark.getString("app-name")
  }

  /*
  This method gives the MongoDB's connection URI taken from the config file
  return: String value of the URI
   */
  def getParquetDestFile(): String = {
    getFile_Destination().getString("parquet.directory") +
      getFile_Destination().getString("parquet.filename")


  }

  // -- Private methods

  /*
  This method gives the spark section taken from the config file
  return: Config object of the spark config
   */
  private def getSpark(): Config = {
    config.getConfig("com.lc.app.spark")
  }

  /*
  This method gives the mongo mapping section taken from the config file
  return: Config object of the Mongo Mapping collection connection config
   */
  private def getMongo_Mapping(): Config = {
    config.getConfig("com.lc.app.mongo-mapping")
  }

  /*
  This method gives the mongo lake section taken from the config file
  return: Config object of the Mongo lake collection's connection config
   */
  private def getMongo_Lake(): Config = {
    config.getConfig("com.lc.app.mongo-lake")
  }

  /*
  This method gives the File process's destination details taken from the config file
  return: Config object of the file-destination collection's connection config
   */
  private def getFile_Destination(): Config = {
    config.getConfig("com.lc.app.file-destination")
  }

  /*
  This method gives the S3 Configs for the S3ClientObject connection
   */
   def getS3Configuration(): Config = {
    config.getConfig("com.lc.app.s3")
  }

   def getFailedQueueConfig() : Config = {
     config.getConfig("com.lc.app.failedQueue")
   }

}

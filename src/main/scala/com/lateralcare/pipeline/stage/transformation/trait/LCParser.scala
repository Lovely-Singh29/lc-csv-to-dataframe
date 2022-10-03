package com.lateralcare.pipeline.stage.transformation.`trait`

import org.apache.spark.sql.{DataFrame, SparkSession}

trait LCParser {

  def setsparkSession(spark: SparkSession): Unit

  def setfilepath(filepath: String): Unit

  def parse(): DataFrame
}

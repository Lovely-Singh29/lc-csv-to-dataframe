package com.lateralcare.pipeline.common

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.rdd.api.java.JavaMongoRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.bson.Document

class DataAccessManager {
  def main(args: Array[String]): Unit = {

    val filepath: String = "D:/lateral care/untitled/src/main/resources/parquetfile/MAP0000112/sampledata.parquet"

    // -- Build spark session and load MongoDB Collection
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("untitled")
      .config("spark.mongodb.input.uri", "mongodb://localhost:27017/lateralcareconfig.fieldmappings")
      .getOrCreate()

    readMongoDB(spark)

  }

  def readMongoDB(spark: SparkSession) {
    val jsc: JavaSparkContext = new JavaSparkContext(spark.sparkContext)
    val rdd: JavaMongoRDD[Document] = MongoSpark.load(jsc)
    println("Total Values " + rdd.count)
    rdd.toDF.show(truncate = false)


  }

}

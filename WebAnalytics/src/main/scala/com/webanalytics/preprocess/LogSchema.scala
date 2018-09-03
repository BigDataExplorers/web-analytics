package com.webanalytics.preprocess
import org.apache.spark.sql.types._

object LogSchema {
  /**
   * The Schema for the log file is given here
   */
  val logFileSchema = StructType(
    StructField("host", StringType, true) ::
      StructField("hyphen_string1", StringType, true) :: //Both empty strings are given here so 
      StructField("hyphen_string2", StringType, true) :: //that later they can be replaced
      StructField("timestamp", StringType, true) ::
      StructField("timezone", StringType, true) ::
      StructField("request", StringType, true) ::
      StructField("http_response_code", IntegerType, true) ::
      StructField("bytes", IntegerType, true) :: Nil)

}
package com.webanalytics.insight_creation

import org.apache.spark.sql.types._

/**
 * The object defines the schema for the session file data
 *
 * @author  Naveen Srinivasan
 * @version 1.0
 * @since   2018-09-06
 */
object SessionFileSchema {
  /**
   * The Schema for the session file is given here
   */
  val sessionFileSchema = StructType(
    StructField("encrypted_host", StringType, true) ::
      StructField("datetime", TimestampType, true) :: 
      StructField("epoch", IntegerType, true) ::
      StructField("request", StringType, true) ::
      StructField("http_response_code", IntegerType, true) ::
      StructField("bytes", IntegerType, true) ::
      StructField("request_url", StringType, true) ::
      StructField("country", StringType, true) ::
      StructField("year", IntegerType, true) ::
      StructField("month", IntegerType, true) ::
      StructField("day", IntegerType, true) ::
      StructField("session_time_difference", IntegerType, true) ::
      StructField("is_new_session", IntegerType, true) :: 
      StructField("session_id", StringType, true) ::
      StructField("first_visited_page", StringType, true) ::
      StructField("last_visited_page", StringType, true) ::
      StructField("page_count", IntegerType, true) :: Nil)
}

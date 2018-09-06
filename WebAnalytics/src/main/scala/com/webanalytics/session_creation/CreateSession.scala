package com.webanalytics.session_creation
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
/**
 * The object creates the session from the preprocessed log data
 *
 * @author  Naveen Srinivasan
 * @version 1.0
 * @since   2018-09-05
 */
object CreateSession {

  /**
   * The main method for the session Creation module
   */
  def main(args: Array[String]) {

    val warehouseLocation = "spark-warehouse"

    val spark = SparkSession
      .builder()
      .appName("Session Creation")
      //.master("local")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    import spark.sql
    val session_creation_sql = """SELECT *, COUNT(last_visited) OVER (PARTITION BY session_id ORDER BY datetime) as page_count FROM(
        SELECT *, first_value(request_url) OVER (PARTITION BY session_id ORDER BY datetime) as first_visited,
        last_value(request_url) OVER (PARTITION BY session_id ORDER BY datetime) as last_visited 
        FROM( SELECT *,CONCAT(encrypted_host, CONCAT('_', SUM(new_session) 
        OVER (PARTITION BY encrypted_host ORDER BY datetime))) AS session_id 
        FROM (SELECT *,  epoch - LAG(epoch) 
        OVER (PARTITION BY encrypted_host ORDER BY datetime) as time_difference, 
        CASE WHEN epoch - LAG(epoch) OVER (PARTITION BY encrypted_host ORDER BY datetime) >= 30 * 60 
        THEN 1 ELSE 0 END AS new_session FROM web_log_analytics.partitioned_log_data) s1)s2)s3
        ORDER BY s3.session_id ASC, s3.datetime ASC"""

    val logWithSessionDf = sql(session_creation_sql)
    logWithSessionDf.write.mode(SaveMode.Overwrite).csv(args(0) + "/session_data")

  }
}
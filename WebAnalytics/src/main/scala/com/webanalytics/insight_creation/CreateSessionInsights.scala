package com.webanalytics.insight_creation

import org.apache.spark.{ SparkConf, SparkContext }
import com.webanalytics.insight_creation.SessionFileSchema.sessionFileSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{avg, desc,row_number,round}
import org.apache.spark.sql.expressions.Window

/**
 * The object reads the log file and preprocess the data
 *
 * @author  Naveen Srinivasan
 * @version 1.0
 * @since   2018-09-03
 */
object CreateSessionInsights {

  /**
   * The main method for the Insights Creation module
   */
  def main(args: Array[String]) {

    // Creating the spark session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Insight Creation")
      .getOrCreate()
      
    import spark.implicits._
    // Read the session file and create a dataframe
    val sessionFileDf = spark.read.option("delimiter", ",")
      .schema(sessionFileSchema).csv(args(0))

    /**
     * Below are the insights for the site    
     */
    // Number of unique visitors for the site
    val uniqueVisitors = sessionFileDf.select("encrypted_host").distinct().count()
    
    // Total number of user sessions
    val totalUserSessions = sessionFileDf.select("session_id").distinct().count()
    
    // Average session duration
    val sumSessionDuration = sessionFileDf.filter(($"session_time_difference" !== null) || 
        ($"session_time_difference" <= 1800)).groupBy("session_id").sum("session_time_difference")
    val avgSessionDuration = sumSessionDuration.select(avg($"sum(session_time_difference)")).as("average").first().getDouble(0).round
    
    // Total number of page views
    val pageViews = sessionFileDf.select("request_url").count()
    
    // Popular pages
    val pageHitCount = sessionFileDf.groupBy("request_url").count().sort(desc("count")).toDF(Seq("request_url","count_page_visit"): _*)
    pageHitCount.createOrReplaceTempView("pageHitCount")
    val topPageCount = args(1).toInt
    val popularPages = spark.sql(f"SELECT * FROM pageHitCount LIMIT $topPageCount")
    
    // Exit Rate Calculation
    val w = Window.partitionBy($"session_id").orderBy($"datetime".desc)

    val lastPages = sessionFileDf.select("datetime","session_id", "last_visited_page").withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")
    val lastPageCount = lastPages.groupBy("last_visited_page").count().sort(desc("count")).toDF(Seq("request_url","count_last_visit"): _*)
    val lastPageHits = lastPageCount.join(pageHitCount, "request_url")
    val exitRates = lastPageHits.withColumn("exit_rate", round((($"count_last_visit" / $"count_page_visit") * 100),2)).sort(desc("exit_rate")).toDF()
    exitRates.createOrReplaceTempView("exitRates")
    val topExitCount = args(2).toInt
    val topExitPages = spark.sql(f"SELECT * FROM exitRates LIMIT $topExitCount")
    
    popularPages.show()
    topExitPages.show()
    
    
    

    
  }
}
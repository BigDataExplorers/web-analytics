package com.webanalytics.insight_creation

import org.apache.spark.{ SparkConf, SparkContext }
import com.webanalytics.insight_creation.SessionFileSchema.sessionFileSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{ Dataset, Row }
import org.apache.spark.sql.functions.{ avg, desc, row_number, round, col, lit, not }
import org.apache.spark.sql.expressions.Window
import java.text.SimpleDateFormat
import java.util.Date
import org.joda.time.DateTime
import scala.math.BigDecimal
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount

/**
 * The object reads the log file and preprocess the data
 *
 * @author  Naveen Srinivasan
 * @version 1.0
 * @since   2018-09-03
 */
class CreateStoreInsights {

  def createStoreInsights(spark: SparkSession, sessionizedData: Dataset[Row], insightDate: String, topPageCnt: Int, topExitCnt: Int, numDaysForNewUSer: Int): (Dataset[Row], Dataset[Row], Dataset[Row]) = {
    import spark.implicits._
    import org.apache.spark.sql.DataFrame
    val sessionFileDf = sessionizedData

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
    val avgSessionDuration = BigDecimal(sumSessionDuration.select(avg($"sum(session_time_difference)")).as("average")
      .first().getDouble(0) / 60).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    // Total number of page views
    val pageViews = sessionFileDf.select("request_url").count()

    //Bounce Rate
    val bouncedSessionsCount = sessionFileDf.groupBy("session_id").count().filter($"count" === 1).count()
    val bounceRate = BigDecimal(((bouncedSessionsCount * 1.0 / totalUserSessions) * 100)).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    // Traffic by countries
    val topCountries = sessionFileDf.groupBy("country").count().sort(desc("count"))

    // Popular pages
    val pageHitCount = sessionFileDf.groupBy("request_url").count().sort(desc("count")).toDF(Seq("request_url", "page_request_count"): _*)
    pageHitCount.createOrReplaceTempView("pageHitCount")
    val topPageCount = topPageCnt
    val popularPages = spark.sql(f"""SELECT 'Page_Hits' as query_type, request_url, page_request_count FROM pageHitCount LIMIT $topPageCnt""")

    // Exit Rate Calculation
    val w = Window.partitionBy($"session_id").orderBy($"datetime".desc)

    val lastPages = sessionFileDf.select("datetime", "session_id", "last_visited_page").withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")
    val lastPageCount = lastPages.groupBy("last_visited_page").count().sort(desc("count")).toDF(Seq("request_url", "count_last_visit"): _*)
    val lastPageHits = lastPageCount.join(pageHitCount, "request_url")
    val exitRates = lastPageHits.withColumn("exit_rate", round((($"count_last_visit" / $"page_request_count") * 100), 2)).sort(desc("exit_rate")).toDF()
    exitRates.createOrReplaceTempView("exitRates")
    val topExitCount = topExitCnt
    val topExitPages = spark.sql(f"""SELECT 'Exit_Rate' as query_type, request_url, exit_rate FROM exitRates LIMIT $topExitCnt""")

    // New users to the site
    val historicalHostsData = spark.sql("SELECT * FROM web_log_analytics.ext_session_data")
    //val historicalHostsData = spark.read.option("delimiter", ",")
      //.schema(sessionFileSchema).csv("src/main/resources/log_data/session_data.csv")
    val histSessionFileWithDate = historicalHostsData.withColumn("dateColumn", historicalHostsData("datetime").cast(DateType))
    val runDate = java.time.LocalDate.parse(insightDate).toString()
    val historyDate = new DateTime(runDate).minusDays(numDaysForNewUSer).toString().substring(0, 10)
    val todayData = sessionFileDf.select("encrypted_host").distinct()
    todayData.createOrReplaceTempView("today_data")
    val uniqueHostsHistory = histSessionFileWithDate.filter($"dateColumn" >= historyDate && $"dateColumn" < runDate)
    val historicalHosts = uniqueHostsHistory.select("encrypted_host").rdd.map(r => r(0)).collect.toList.mkString("'", "','", "'")
    val newUs = spark.sql(f"""SELECT encrypted_host FROM today_data WHERE encrypted_host NOT IN ($historicalHosts)""")
    val newUsers = newUs.count()

    // Preparing data for the Daily Insight Table

    val someSchema = List(
      StructField("insight_year", IntegerType, true),
      StructField("insight_month", IntegerType, true),
      StructField("insight_day", IntegerType, true),
      StructField("insight_date", StringType, true),
      StructField("unique_visitor_count", LongType, true),
      StructField("user_session_count", LongType, true),
      StructField("avg_session_duration", DoubleType, true),
      StructField("page_views_count", LongType, true),
      StructField("bounce_count", LongType, true),
      StructField("bounce_rate", DoubleType, true),
      StructField("new_users_count", LongType, true))

    val insight_year = runDate.split("-")(0).toInt
    val insight_month = runDate.split("-")(1).toInt
    val insight_day = runDate.split("-")(2).toInt

    val siteData = Seq(Row(insight_year, insight_month, insight_day, insightDate, uniqueVisitors, totalUserSessions, avgSessionDuration, pageViews, bouncedSessionsCount, bounceRate, newUsers))
    val siteInsights = spark.createDataFrame(spark.sparkContext.parallelize(siteData), StructType(someSchema))

    // Preparing Data for the Popular / High Exit Rate table
    val popularAndExitDf = popularPages.unionAll(topExitPages)
    val popularAndExitTable = popularAndExitDf.withColumn("insight_date", lit(runDate)).toDF(Seq("query_type", "request_url", "measure_value", "insight_date"): _*)
      .select("insight_date", "query_type", "request_url", "measure_value")

    // Preparing data for Country Traffic
    val countryTraffic = topCountries.withColumn("insight_date", lit(runDate)).toDF(Seq("country_name", "page_request_count", "insight_date"): _*)
      .select("insight_date", "country_name", "page_request_count")

    return (siteInsights, popularAndExitTable, countryTraffic)
  }

}

package com.webanalytics.insight_creation
import org.apache.spark.{ SparkConf, SparkContext }
import com.webanalytics.insight_creation.SessionFileSchema.sessionFileSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.{ Dataset, Row }
import org.apache.spark.sql.functions.{ avg, desc, row_number, round }
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
      //.enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    // Read the session file and create a dataframe
    val sessionFileDf = spark.read.option("delimiter", ",")
      .schema(sessionFileSchema).csv(args(0))

    val sessionFileWithDate = sessionFileDf.withColumn("dateColumn", sessionFileDf("datetime").cast(DateType))
    val date = args(1)
    val topPageCount = args(2).toInt
    val topExitCount = args(3).toInt
    val numDaysForNewUSer = args(4).toInt
    val selected_data = sessionFileWithDate.filter($"dateColumn" === date)
    val insights = new CreateStoreInsights()
    val (siteInsights, popularAndExitTable, countryTraffic) = insights.createStoreInsights(spark, selected_data, date, topPageCount, topExitCount, numDaysForNewUSer)
    siteInsights.show()
    popularAndExitTable.show()
    countryTraffic.show()
    //print(selected_data.count())

  }
}
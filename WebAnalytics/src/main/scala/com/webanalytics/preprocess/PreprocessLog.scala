package com.webanalytics.preprocess

import org.apache.spark.{ SparkConf, SparkContext }
import com.webanalytics.preprocess.LogSchema.{ logFileSchema, ipMappingFileSchema }
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{ udf, regexp_extract, translate, broadcast, year, month, dayofmonth, unix_timestamp}
import org.apache.hadoop.hdfs.server.namenode.INodeReference.WithCount
import com.webanalytics.preprocess.UserDefinedFunctions.{ getUrl, ipToIntUDF, hostToCountry, encryptUrl, timeStampToDate}
import org.apache.spark.sql.SaveMode

/**
 * The object reads the log file and preprocess the data
 *
 * @author  Naveen Srinivasan
 * @version 1.0
 * @since   2018-09-03
 */
object PreprocessLog {

  /**
   * The main method for the preprocessing module
   */
  def main(args: Array[String]) {

    // Creating the spark session
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Web Log Preprocess")
      .getOrCreate()
      
    spark.conf.set("spark.sql.broadcastTimeout", -1)
    import spark.implicits._

    // Read the delimited file and create a dataframe
    val logFileDf = spark.read.option("delimiter", " ")
      .schema(logFileSchema).csv(args(0))

    // Get the dirty records and store separately
    val nullLogRecordDf = logFileDf.filter("host is null or hyphen_string1 is null or hyphen_string2 is null or timestamp is null or timezone is null or request is null or http_response_code is null or bytes is null")
    nullLogRecordDf.write.mode(SaveMode.Overwrite).csv(args(2) + "/null_records")

    // Get the not null records
    val notNullLogRecordDf = logFileDf.filter("host is not null and timestamp is not null and request is not null and http_response_code is not null and bytes is not null")

    val logFileEncrypted = notNullLogRecordDf.withColumn("encrypted_host", encryptUrl($"host"))

    //Taking the necessary fields
    val logDfSelDf = logFileEncrypted.select("host", "encrypted_host", "timestamp", "request", "http_response_code", "bytes")

    val logDfDateCleaned = logDfSelDf.select($"host", $"encrypted_host", translate($"timestamp", "[", "").as("timestamp"), $"request", $"http_response_code", $"bytes")
    // Filtering records with proper urls
    val urlRegex = """^([A-Z])* (\/([a-zA-Z0-9\-\.\_\?\/\+\%\~\:]+)?)*(\.{1}([a-z0-9]+)*)?( [A-Z0-9]+\/[0-9|a-z|A-Z]+\.[0-9|a-z|A-Z])?$"""
    val urlCleanDf = logDfDateCleaned.filter($"request".rlike(urlRegex))

    //Filtering the redirect
    val rejectDf = logDfDateCleaned.filter(!$"request".rlike(urlRegex))
    rejectDf.select("encrypted_host", "timestamp", "request", "http_response_code", "bytes").write.mode(SaveMode.Overwrite).csv(args(2) + "/redirect_records")

    //Deriving the requested resource from the url request
    val logFileReqDf = urlCleanDf.withColumn("request_url", getUrl($"request"))
    logFileReqDf.createOrReplaceTempView("log_with_request")

    // Filtering the requests with just the images
    // MSFC, JPG, JPEG, MPG, GIF, XBM
    val mediaFilteredDf = logFileReqDf.filter($"request_url".rlike("""^\/([a-z|A-Z]|\/|\-|[0-9]|\_)*.?(html|htm|\?|[0-9]*\,[0-9]*)?$"""))
    mediaFilteredDf.select("encrypted_host", "timestamp", "request", "http_response_code", "bytes").write.mode(SaveMode.Overwrite).csv(args(2) + "/non_media_records")

    //Saving the media records for future reference
    val mediaDataDf = logFileReqDf.filter(!$"request_url".rlike("""^\/([a-z|A-Z]|\/|\-|[0-9]|\_)*.?(html|htm|\?|[0-9]*\,[0-9]*)?$"""))
    mediaDataDf.select("encrypted_host", "timestamp", "request", "http_response_code", "bytes").write.mode(SaveMode.Overwrite).csv(args(2) + "/media_records")

    //Reading the country ip mapping file
    val ipFileDf = spark.read.option("delimiter", ",")
      .schema(ipMappingFileSchema).csv(args(1))

    ipFileDf.createOrReplaceTempView("ip_country_data")
    val sql_sel_query = "SELECT begin_ip_int, end_ip_int, LOWER(country_code) AS country_code, country FROM ip_country_data"
    val ipFileReqDf = spark.sql(sql_sel_query)
    ipFileReqDf.createOrReplaceTempView("ip_country_mapping_data")

    //Getting the IP values that have numbers
    val logsWithNumIp = mediaFilteredDf.filter($"host".rlike("""^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$"""))

    //Getting the IP values that have strings
    val logsWithAlphIp = mediaFilteredDf.filter($"host".rlike("""^([a-z]|[0-9]|\-|\_|\.)*\.([a-z])*$"""))

    //Calling the UDF to get the integer value of IP
    val ipComputedDf = logsWithNumIp.withColumn("ipInteger", ipToIntUDF($"host"))
    ipComputedDf.createOrReplaceTempView("log_with_long_ip")

    // Joining with country mapping table to get the country name of the IP
    val longIpHostCountryQuery = """SELECT log.host, ip.country FROM log_with_long_ip log, ip_country_mapping_data ip WHERE log.ipInteger BETWEEN ip.begin_ip_int AND ip.end_ip_int"""
    val ipCountryDf = spark.sql(longIpHostCountryQuery)

    //Getting the country code from the URL
    val logsWithAlphIpWithCode = logsWithAlphIp.withColumn("country_code", hostToCountry($"host"))
    logsWithAlphIpWithCode.createOrReplaceTempView("log_with_cntry_code")

    // Taking the distinct records for better mapping
    val distCntrys = ipFileReqDf.select("country_code", "country").distinct()

    //Joining with the mapping table to get countries for hostnames
    val hostCountryDf = logsWithAlphIpWithCode.join(distCntrys, logsWithAlphIpWithCode("country_code") === distCntrys("country_code"), "left").select("host", "country")
    val map = Map("country" -> "NA")
    val hostCountryDfnotNA = hostCountryDf.na.fill(map)

    // Concatenating the host and country mappig file for both the IPs with numbers and strings
    val completeHostCountry = ipCountryDf.union(hostCountryDfnotNA).distinct()
    val newNames = Seq("host_name", "country")
    val completeHostCountryRenamed = completeHostCountry.toDF(newNames: _*)

    // Joining with the main logs to map countries
    val countryMappedLog = mediaFilteredDf.join(broadcast(completeHostCountryRenamed), mediaFilteredDf("host") === completeHostCountryRenamed("host_name"), "left")
    val logWithTimestamp = countryMappedLog.withColumn("datetime", unix_timestamp($"timestamp","dd'/'MMM'/'yyyy':'HH':'mm':'ss").cast(TimestampType))
    val logWithTimeEpoch = logWithTimestamp.withColumn("epoch", $"datetime".cast("long")).withColumn("year", year($"datetime")).withColumn("month", month($"datetime")).withColumn("day", dayofmonth($"datetime"))

    logWithTimeEpoch.select("encrypted_host", "datetime", "epoch", "request", "http_response_code", "bytes", "request_url", "country", "year", "month", "day").write.mode(SaveMode.Overwrite).csv(args(2) + "/ip_country_mapping")

  }

}

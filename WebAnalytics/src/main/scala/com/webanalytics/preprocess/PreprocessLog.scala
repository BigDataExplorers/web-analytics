import org.apache.spark.{ SparkConf, SparkContext }
import com.webanalytics.preprocess.LogSchema.logFileSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{ udf, regexp_extract }

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
      .appName("Read and preprocess the log file")
      .getOrCreate()
    import spark.implicits._

    // Read the delimited file and create a dataframe
    val logFileDf = spark.read.option("delimiter", " ")
      .schema(logFileSchema).csv(args(0))

    // Get the dirty records and store separately
    val nullLogRecords = logFileDf.filter("host is null or timestamp is null or request is null or http_response_code is null or bytes is null")
    nullLogRecords.write.csv(args(1) + "/null_records")

    // Get the not null records
    val notNullLogRecords = logFileDf.filter("host is not null and timestamp is not null and request is not null and http_response_code is not null and bytes is not null")
    notNullLogRecords.show(10)

    //Taking the necessary fields
    notNullLogRecords.registerTempTable("log_file")
    val sql_query = "SELECT host, timestamp, request, http_response_code, bytes FROM log_file"
    val logDfSelected = spark.sql(sql_query)

    // Filtering records with proper urls
    val urlRegex = """^([A-Z])* (\/[a-z0-9]+)*(\.{1}([a-z0-9]+)*)? [A-Z0-9]+\/[0-9]\.[0-9]$"""
    val urlCleanData = logDfSelected.filter($"request" rlike urlRegex)
    //val df1 = logDfSelected.withColumn("request_url", regexp_extract(logDfSelected("request"), urlRegex, 1))

    /**
     * udf to filter the request url from the request body
     */
    def getUrl = udf((rawData: String) =>
      {
        rawData.split(" ")(1)
      })

    val logFileWithRequest = urlCleanData.withColumn("request_url", getUrl($"request"))
    logFileWithRequest.registerTempTable("log_with_request")

    // Filtering the requests with just the images
    // MSFC, JPG, JPEG, MPG, GIF, XBM    
    val mediaFilteredData = logFileWithRequest.filter(!$"request_url".rlike("(.jpg|.gif|.jpeg|.mpg|.mfsc|.xbm)$"))
    mediaFilteredData.show()
    mediaFilteredData.write.csv(args(1) + "/non_media_records")
    
    //Saving the media records for future reference
    val mediaData = logFileWithRequest.filter($"request_url".rlike("(.jpg|.gif|.jpeg|.mpg|.mfsc|.xbm)$"))
    mediaData.write.csv(args(1) + "/media_records")
    
  }

}

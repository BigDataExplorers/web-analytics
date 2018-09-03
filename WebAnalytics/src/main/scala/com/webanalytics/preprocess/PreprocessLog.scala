import org.apache.spark.{ SparkConf, SparkContext }
import com.webanalytics.preprocess.LogSchema.logFileSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext

object PreprocessLog {
  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Preprocess Log File")
      .getOrCreate()
    import spark.implicits._

    val logFileDf = spark.read.option("delimiter", " ").schema(logFileSchema).csv("src/main/resources/log_data/access_log_Jul95.csv")
    val columnNames = logFileDf.schema.fieldNames
    logFileDf.printSchema()
    logFileDf.show()

  }

}

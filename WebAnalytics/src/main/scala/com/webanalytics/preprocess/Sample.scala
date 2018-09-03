import org.apache.spark.{ SparkConf, SparkContext }
import com.webanalytics.preprocess.LogSchema.logFileSchema
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf

/**
 * The object reads the log file and preprocess the data
 *
 * @author  Naveen Srinivasan
 * @version 1.0
 * @since   2018-09-03
 */
object Sample {

  /**
   * The main method for the preprocessing module
   */
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Sample").setMaster("local")
    val sc = new SparkContext(conf)
    
    val logFile = sc.textFile(args(0))
    logFile.foreach(println)
    
    
    
    

  }
}

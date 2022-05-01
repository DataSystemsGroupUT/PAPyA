package loader.extvp

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Different settings for the DataSetGenerator
 * TODO: implement reading of settings from config file.
 */
object extvpSettings {
  //val sparkContext = loadSparkContext()
 // var sqlContext = loadSqlContext()
  var sparksession = loadSparkSession()
  // ScaleUB (Scale Upper Bound) controls the storage overhead.
  // Set it to 1 (default) to store all possible ExtVP tables. Reduce this value
  // to avoid storage of ExtVP tables having size bigger than ScaleUB * (size of
  // corresponding VP table). In this way, we avoid storage of the biggest
  // tables, which are most ineffective at the same time, since they are
  // not able to significantly improve the selectivity of correponding triple
  // pattern
  var ScaleUB = 1:Float

  // database name
  //val baseName = "WatDiv1000M_test"
  // the database directory in HDFS
  var workingDir = ""
  // path to the input RDF file
  var inputRDFSet = ""
  // path to Parquet file for the Triple Table
  var tripleTable = ""
  // path to the directory for all VP tables
  var vpDir = ""
  // path to the directory for all ExtVP tables
  var extVpDir = ""
  // path to the directory for all WPT tables
  var wptDir = ""


  def loadUserSettings(inFilePath:String,
                   inFileName:String,
                   ds:String,
                   scale:Float) = {
    this.ScaleUB = scale
    this.workingDir = inFilePath
    this.inputRDFSet = inFilePath + inFileName

    this.tripleTable = this.workingDir + s"tripletable/"
    this.vpDir = this.workingDir + "VP/"
    this.extVpDir = this.workingDir + "ExtVP/"
    this.wptDir=this.workingDir + "WPT/"
  }


/**
* Create SparkSession
*/

def loadSparkSession(): SparkSession = {


  val conf = new SparkConf().setAppName("DataSetsCreator")
    .set("spark.executor.memory", "100g")
    .set("spark.driver.memory","50g")
    .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
    .set("spark.sql.parquet.filterPushdown", "true")
    .set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
    .set("spark.storage.blockManagerSlaveTimeoutMs", "3000000")
    .set("spark.storage.memoryFraction", "0.5")


val spark = SparkSession
  .builder()
//  .master("spark://172.17.67.122:7077")
  .master("local[*]")
  .config(conf)
  .getOrCreate()
  println("spark-session created")
  spark.sparkContext.getConf.getAll.foreach(println(_))

spark
}

}

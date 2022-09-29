package loader.extvp


import java.nio.file.{Files, Paths}
import scala.collection.mutable.HashMap

object DataSetGenerator {

  private val _sparksession = extvpSettings.sparksession
  _sparksession.sparkContext.setLogLevel("WARN")

  // number of triples in input dataset
  private var _inputSize = 0: Long
  // number of triples for every VP table
  private var _vpTableSizes = new HashMap[String, Long]()
  // set of unique predicates from input RDF dataset
  private var _uPredicates = null: Array[String]

  def getUniquePreds(): Array[String] = {

    /**
     * General UseCase (All Distinct preds)
     */
    _uPredicates = _sparksession.sql("select distinct p from triples").rdd
      .map(t => t(0).toString())
      .collect()
    _uPredicates
  }

  def generateDataSet(datasetType: String) = {

    loadTT()
    // extarct all unique predicates from TripleTable, necessary for VP/ExtVP generation
    _uPredicates = getUniquePreds()

    StatisticWriter.init(_uPredicates.size, _inputSize)

    // create or load Vertical Partitioning if already exists
    if (datasetType == "VP") try {
      loadVP()
    } catch {
      case e: org.apache.spark.sql.AnalysisException => createVP()
    }


    // create Extended Vertical Partitioning table set defined by datasetType
    if (datasetType == "SO") createExtVP("SO")
    else if (datasetType == "OS") createExtVP("OS")
    else if (datasetType == "SS") createExtVP("SS")
  }

  // Triple Table schema
  case class Triple(sub: String, pred: String, obj: String)

  /**
   * Generate TripleTable and save it to Parquet file in HDFS.
   * The table has to be cached, since it is used for generation of VP and ExtVP
   */
  def createTT() = {

    val df = _sparksession.read.format("csv").option("header", "true").option("inferSchema", "true").load(extvpSettings.inputRDFSet).toDF()

    // Commented out due to execution problem for dataset of 1 Bil triples
    // We do not need it anyway if the input dataset is correct and has no
    // double elements. It was not the case for WatDiv
    //                     .distinct
    df.createOrReplaceTempView("triples")
    _sparksession.table("triples").cache()
    _inputSize = df.count()

    // remove old TripleTable and save it as Parquet
    // Helper.removeDirInHDFS(Settings.tripleTable)

    df.write.parquet(extvpSettings.tripleTable)

  }

  /**
   * Loads TT table and caches it to main memory.
   * TT table is used for generation of ExtVP and VP tables
   */
  def loadTT() = {

    val df = _sparksession.read.format("parquet").option("header", "true").option("inferSchema", "true").load(extvpSettings.tripleTable)
    df.createOrReplaceTempView("triples")
    _sparksession.table("triples").cache()
  }


  /**
   * Generates VP table for each unique predicate in input RDF dataset.
   * All tables have to be cached, since they are used for generation of ExtVP
   * tables.
   */
  def createVP() = {
    // create directory for all vp tables

    StatisticWriter.initNewStatisticFile("VP")

    // create and cache vpTables for all predicates in input RDF dataset

    _uPredicates = getUniquePreds()

    _uPredicates.foreach(println)

    for (predicate <- _uPredicates) {
      var vpcommand = "select s, o from triples where p='" + predicate + "'"

      var vpTable = _sparksession.sql(vpcommand)
      val cleanPredicate = Helper.getValidPredName(predicate)

      vpTable.createOrReplaceTempView(cleanPredicate)
      _sparksession.table(cleanPredicate).cache()
      _vpTableSizes(predicate) = vpTable.count()

      vpTable.coalesce(1).write.parquet(extvpSettings.vpDir + cleanPredicate)

      println("VP Tables of " + cleanPredicate + " are Created (All formats) !")

      // print statistic line
      StatisticWriter.incSavedTables()
      StatisticWriter.addTableStatistic("<" + predicate + ">", -1, _vpTableSizes(predicate))
    }

    StatisticWriter.closeStatisticFile()
  }

  /**
   * Loads VP tables and caches them to main memory.
   * VP tables are used for generation of ExtVP tables
   */
  private def loadVP() = {

    _uPredicates = getUniquePreds()

    for (predicate_uri <- _uPredicates) {
      val cleanPredicate = Helper.getValidPredName(predicate_uri)

      var vpTable = _sparksession.read.parquet(extvpSettings.vpDir
        + cleanPredicate)

      vpTable.createOrReplaceTempView(cleanPredicate)
      _sparksession.table(cleanPredicate).cache()
      _vpTableSizes(predicate_uri) = vpTable.count()

    }
    println("VP Tables are Loaded!")
  }


  /**
   * Generates ExtVP tables for all (relType(SO/OS/SS))-relations of all
   * VP tables to the other VP tables
   */
  def createExtVP(relType: String) = {

    /**
     * Move the generated VP Tables into a VP directory
     */
    val src_path = extvpSettings.workingDir
    val tgt_path = Files.createDirectories(Paths.get(extvpSettings.workingDir + "VP"))

    val listOfVPs = Helper.getListOfVPDirs(src_path)

    if (listOfVPs.length > 0) {

      listOfVPs.foreach { f =>
        val src_file = f.toString()
        val tgt_file = tgt_path + "/" + f.getName.replace("vp_", "")

        //move Renamed VPs (src_file,tgt_file)
        Helper.moveRenameFile(src_file, tgt_file)
        println("File Copied : " + tgt_file)
      }
    }

    /**
     * Load the VP Tables from the VP directory
     */
    loadVP()


    // create directory for all ExtVp tables of given relType (SO/OS/SS)
    // Helper.createDirInHDFS(Settings.extVpDir+relType)

    StatisticWriter.initNewStatisticFile(relType)

    var savedTables = 0
    var unsavedNonEmptyTables = 0
    var createdDirs = List[String]()

    // for every VP table generate a set of ExtVP tables, which represent its
    // (relType)-relations to the other VP tables

    _uPredicates = getUniquePreds()

    for (pred1 <- _uPredicates) {

      // get all predicates, whose TPs are in (relType)-relation with TP
      // (?x, pred1, ?y)


      var relatedPredicates = getRelatedPredicates(pred1, relType)


      for (pred2 <- relatedPredicates) {
        var extVpTableSize = -1: Long

        // we avoid generation of ExtVP tables corresponding to subject-subject
        // relation to it self, since such tables are always equal to the
        // corresponding VP tables

        if (!(relType == "SS" && pred1 == pred2)) {

          var sqlCommand = getExtVpSQLcommand(pred1, pred2, relType)

          var extVpTable = _sparksession.sql(sqlCommand)

          extVpTable.createOrReplaceTempView("extvp_table")
          // cache table to avoid recomputation of DF by storage to HDFS
          _sparksession.table("extvp_table").cache()

          extVpTableSize = extVpTable.count()

          // save ExtVP table in case if its size smaller than
          // ScaleUB*size(corresponding VPTable)
          if (extVpTableSize < _vpTableSizes(pred1) * extvpSettings.ScaleUB) {

            // create directory extVP/relType/pred1 if not exists
            if (!createdDirs.contains(pred1)) {
              createdDirs = pred1 :: createdDirs
              Helper.getValidPredName(extvpSettings.extVpDir
                + relType + "/"
                + Helper.getValidPredName(pred1))
            }

            // save ExtVP table AS Parquet
            extVpTable.coalesce(1).write.parquet(extvpSettings.extVpDir
              + relType + "/"
              + Helper.getValidPredName(pred1) + "/"
              + Helper.getValidPredName(pred2)
            )

            println("EXTVP Tables of " + relType + "==>"
              + Helper.getValidPredName(pred1) + "/"
              + Helper.getValidPredName(pred2) + " are Created in Parquet !")

            StatisticWriter.incSavedTables()
          } else {
            StatisticWriter.incUnsavedNonEmptyTables()
          }

          _sparksession.catalog.uncacheTable("extvp_table")

        }
        else {
          extVpTableSize = _vpTableSizes(pred1)
        }

        // print statistic line
        // save statistics about all ExtVP tables > 0, even about those, which
        // > then ScaleUB.
        // We need statistics about all non-empty tables
        // for the Empty Table Optimization (avoiding query execution for
        // the queries having triple pattern relations, which lead to empty
        // result)
        StatisticWriter.addTableStatistic("<" + pred1 + "><" + pred2 + ">",
          extVpTableSize,
          _vpTableSizes(pred1))
      }

    }

    StatisticWriter.closeStatisticFile()

  }


  /**
   * Returns all predicates, whose triple patterns are in (relType)-relation
   * with TP of predicate pred.
   */
  private def getRelatedPredicates(pred: String, relType: String)
  : Array[String] = {


    var sqlRelPreds = ("select distinct p from triples t1 left semi join " + Helper.getValidPredName(pred) + " t2 " + "on")

    if (relType == "SS") {
      sqlRelPreds += "(t1.s=t2.s)"
    } else if (relType == "OS") {
      sqlRelPreds += "(t1.s=t2.o)"
    } else if (relType == "SO") {
      sqlRelPreds += "(t1.o=t2.s)"
    }
    import _sparksession.implicits._
    _sparksession.sql(sqlRelPreds).map(t => t(0).toString()).collect()
  }

  /**
   * Generates SQL query to obtain ExtVP_(relType)pred1|pred2 table containing
   * all triples(pairs) from VPpred1, which are linked by (relType)-relation
   * with some other pair in VPpred2
   */
  private def getExtVpSQLcommand(pred1: String,
                                 pred2: String,
                                 relType: String): String = {
    var command = ("select t1.s, t1.o "
      + "from " + Helper.getValidPredName(pred1) + " t1 "
      + "left semi join " + Helper.getValidPredName(pred2) + " t2 "
      + "on ")

    if (relType == "SS") {
      command += "(t1.s=t2.s)"
    } else if (relType == "OS") {
      command += "(t1.o=t2.s)"
    } else if (relType == "SO") {
      command += "(t1.s=t2.o)"
    }

    command
  }
}

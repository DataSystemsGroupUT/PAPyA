package loader.extvp

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.io.File
import scala.util.Random
import java.text.SimpleDateFormat
import java.util.Date


/**
 * The set of different help-functions
 * TODO: move to the places, where they are used due to small number of
 * functions
 */
object Helper {

  /**
   * transform table name for storage table in HDFS
   */
  def getPartName(v: String): String = {
    v.replaceAll(":", "__").replaceAll("/","").replaceAll("<", "").replaceAll(">", "")
  }




	def getListOfVPDirs(dir: String):List[File] = {
			val d = new File(dir)
					if (d.exists && d.isDirectory) {
						d.listFiles.filter(_.isDirectory).filter(_.getName().contains("vp_")).toList
					} else {
						List[File]()
					}
	}

  	def getListOfDirs(dir: String):List[File] = {
			val d = new File(dir)
					if (d.exists && d.isDirectory) {
						d.listFiles.filter(_.isDirectory).toList
					} else {
						List[File]()
					}
	}


  // Function to move file from source to target - Deletes the source file
  def moveRenameFile(source: String, destination: String): Unit = {
    val path = Files.move(
        Paths.get(source),
        Paths.get(destination),
        StandardCopyOption.REPLACE_EXISTING
    )
  }


  /**
    * transform table name for storage table in HDFS
    */
  def getPredicatesNamefromURI(v: String): String = {

    if(v.contains("#"))
      v.substring(v.lastIndexOf('#')+1)
    else
        v.substring(v.lastIndexOf('/')+1)
  }


  def getValidPredName(v:String):String={
    v.replaceAll("[<>]", "").trim().replaceAll("[[^\\w]+]", "_");
  }


  /**
   * Float to String formated
   */
  def fmt(v: Any): String = v match {
    case d : Double => "%1.2f" format d
    case f : Float => "%1.2f" format f
    case i : Int => i.toString
    case _ => throw new IllegalArgumentException
  }

  /**
   * get ratio a/b as formated string
   */
  def ratio(a: Long, b: Long): String = {
    fmt((a).toFloat/(b).toFloat)
  }

  /**
   * remove directory in HDFS (if not exists -> it's ok :))

  def removeDirInHDFS(path: String) = {
    val cmd = "hdfs dfs -rm -f -r " + path
    val output = cmd.!!
  }
    */
  /**
   * create directory in HDFS

  def createDirInHDFS(path: String) = {
    try{
      val cmd = "hdfs dfs -mkdir " + path
      val output = cmd.!!
    } catch {
      case e: Exception => println("Cannot create directory->"
                                   + path + "\n" + e)
    }
  } */
}

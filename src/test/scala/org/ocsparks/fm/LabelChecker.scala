package org.ocsparks.fm

import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.conf.Configuration



object LabelChecker {
  val conf = new Configuration()
  val hdfsCoreSitePath = new Path("core-site.xml")
  conf.addResource(hdfsCoreSitePath)
  val fs = FileSystem.get(conf);
  
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("TESTFM").setMaster("local[4]"))
    
    val status = fs.listStatus( new Path("/user/adaptive/fm/data") )
    for (i <- 0 until status.length){
      val filePath = status(i).getPath()
    }
  }
  
  def check(filePath : String, sc: SparkContext){
    
    val path = "hdfs://devserver2/user/adaptive/fm/data/" + filePath
    val parsed = sc.textFile(path, 8)
      .map(_.trim)
      .filter(line => !(line.isEmpty || line.startsWith("#")))
      .map { line =>
        val items = line.split(' ')
        try {
        val label = items.head.toDouble
        } catch {
          case e: NumberFormatException => println(filePath + "-" + items.head)
        }
    }
    
    parsed.count
        
    /*val files = new java.io.File("/home/bruce/Documents/machine learning/data").listFiles
    files.foreach { 
      file => 
//        println("file = " + file.getName)
        Source.fromFile(file).getLines()
//        .filter(line => line.startsWith("198:1"))
        .map {
          line => 
            val label = line.split(" ")(0)
            if (label.contains(":")){
              println(line)
            }
        }
      
    }*/
  }
}
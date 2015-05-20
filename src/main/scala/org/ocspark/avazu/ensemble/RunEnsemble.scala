package org.ocspark.avazu.ensemble

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RunEnsemble {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    val sparkConf = new SparkConf().setAppName("Run Ensemble")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    runAll(sc)
    ensemble()
  }
  
  def runAll(sc : SparkContext){
    org.ocspark.avazu.ensemble.util.RunAll.run(sc)
    /*
    val currentDir = new File("./src/main/scala/org/ocspark/avazu/ensemble/model")
    println("current dir = " + currentDir.getCanonicalPath())
    val scalaIndex = currentDir.getCanonicalPath().indexOf("scala")
    val beg = scalaIndex + "scala".length
    val subDirs = recursiveListDirs(currentDir)
    for ( dir <- subDirs){
      println("sub dir = " + dir.getCanonicalPath())
      val packageName = dir.getCanonicalPath().substring(beg)
      println("packageName = " + packageName)
      
    }*/
  }
  
  def ensemble(){
    
  }
  
  /*def recursiveListDirs(f: File): Array[File] = {
	  val subDirs = Array[File]()
	  val these = f.listFiles
	  subDirs ++ these.filter(_.isDirectory)
	}*/

}
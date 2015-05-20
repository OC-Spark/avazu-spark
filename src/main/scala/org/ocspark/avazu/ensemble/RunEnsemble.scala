package org.ocspark.avazu.ensemble

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object RunEnsemble {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    
    val size = args(0)
    
    val sparkConf = new SparkConf().setAppName("Run Ensemble")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    runAll(sc)
    ensemble(size, sc)
  }
  
  def runAll(sc : SparkContext){
    org.ocspark.avazu.ensemble.util.RunAll.run(sc)
  }
  
  def ensemble(size : String, sc : SparkContext){
    org.ocspark.avazu.ensemble.util.Ensemble.run(size, sc)
  }

}
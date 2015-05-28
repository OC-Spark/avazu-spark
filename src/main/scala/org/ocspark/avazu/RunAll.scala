package org.ocspark.avazu

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.ocspark.avazu.base.RunBase
import org.ocspark.avazu.bag.run.RunBag
import org.ocspark.avazu.ensemble.RunEnsemble

object RunAll {

  def main(args: Array[String]): Unit = {
    val size = args(0)
    
        val sparkConf = new SparkConf().setAppName("GenData")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    run(size, sc)
  }
  
  def run(size : String, sc : SparkContext){
    RunBase.run(size, sc)
    RunBag.run(size, 4, sc)
    RunEnsemble.runAll(size, sc)
  }

}
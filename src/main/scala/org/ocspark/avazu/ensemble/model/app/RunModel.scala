package org.ocspark.avazu.ensemble.model.app

import org.ocspark.avazu.ensemble.model.app.util.RunTemplate
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.ocspark.avazu.ensemble.model.app.util.RunTemplate

object RunModel {

  def main(args: Array[String]): Unit = {
    val size = args(0)
        val sparkConf = new SparkConf().setAppName("RunModel")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    RunTemplate.run(size, "a-site_id-85f751fd", "8", sc)
  }
  
  def run(sc : SparkContext){
    RunTemplate.run("x", "a-site_id-85f751fd", "8", sc)
  }
  

}
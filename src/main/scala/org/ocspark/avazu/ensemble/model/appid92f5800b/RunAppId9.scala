package org.ocspark.avazu.ensemble.model.appid92f5800b

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.ocspark.avazu.ensemble.util.RunTemplate

object RunAppId9 {

   def main(args: Array[String]): Unit = {
    val size = args(0)
        val sparkConf = new SparkConf().setAppName("RunModel")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    run("x", sc)
  }
  
  def run(size : String, sc : SparkContext){
    RunTemplate.run(size, "A-app_id-92f5800b", "8", "appId9", new AppId9Cvt(), sc)
  }

} 

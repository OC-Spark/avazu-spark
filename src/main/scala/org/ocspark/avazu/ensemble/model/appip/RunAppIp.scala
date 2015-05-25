package org.ocspark.avazu.ensemble.model.appip

import org.ocspark.avazu.ensemble.util.RunTemplate
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object RunAppIp {

   def main(args: Array[String]): Unit = {
    val size = args(0)
        val sparkConf = new SparkConf().setAppName("RunModel")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    run("x", sc)
  }
  
  def run(size : String, sc : SparkContext){
    RunTemplate.run(size, "A-site_id-85f751fd,A-device_id-a99f214a", "8", "appIp", new AppIpCvt(), sc)
  }

}
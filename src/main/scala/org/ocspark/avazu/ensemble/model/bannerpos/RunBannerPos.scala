package org.ocspark.avazu.ensemble.model.bannerpos

import org.ocspark.avazu.ensemble.util.RunTemplate
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
object RunBannerPos {

   def main(args: Array[String]): Unit = {
    val size = args(0)
        val sparkConf = new SparkConf().setAppName("RunModel")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    run("x", sc)
  }
  
  def run(size : String, sc : SparkContext){
    RunTemplate.run(size, "A-banner_pos-1", "8", "bannerPos", new BannerPosCvt(), sc)
  }

}
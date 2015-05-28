package org.ocspark.avazu.ensemble.util

import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf

object RunAll {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val sparkConf = new SparkConf().setAppName("Ensemble Run All")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
        
    run("x", sc)
  }
  
  def run(size : String, sc : SparkContext){
    org.ocspark.avazu.ensemble.model.app.RunApp.run(size, sc)
    org.ocspark.avazu.ensemble.model.appcategory0f2161f8.RunAppCat.run(size, sc)
    org.ocspark.avazu.ensemble.model.appid.RunAppId.run(size, sc)
    org.ocspark.avazu.ensemble.model.appid92f5800b.RunAppId9.run(size, sc)
    org.ocspark.avazu.ensemble.model.appip.RunAppIp.run(size, sc)
    org.ocspark.avazu.ensemble.model.bannerpos.RunBannerPos.run(size, sc)
    // add future models below
  }

}
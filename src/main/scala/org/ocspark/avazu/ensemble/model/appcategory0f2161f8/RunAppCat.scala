package org.ocspark.avazu.ensemble.model.appcategory0f2161f8

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.ocspark.avazu.ensemble.util.RunTemplate


object RunAppCat {

  def main(args: Array[String]): Unit = {
    val size = args(0)
        val sparkConf = new SparkConf().setAppName("RunModel")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    run("x", sc)
  }
  
  def run(size : String, sc : SparkContext){
    RunTemplate.run(size, "A-app_category-0f2161f8", "8", "appCat", new AppCatCvt(), sc)
  }
  

}
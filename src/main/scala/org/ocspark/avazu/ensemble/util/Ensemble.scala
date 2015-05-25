package org.ocspark.avazu.ensemble.util

import org.apache.spark.SparkContext

object Ensemble {

  def main(args: Array[String]): Unit = {
    val size = args(0)
  }
  
  def run(size : String, sc : SparkContext){
    // iterate over all prd files in pool directory
    MergePrd.run(s"/avazu/r$size.prd", sc)
    
    if (size != "0"){
      val loss = CalcLoss.run(s"/avazu/r$size.prd", s"/avazu/va.r$size.csv", sc)
      println("loss = " + loss)
    }
  }

}
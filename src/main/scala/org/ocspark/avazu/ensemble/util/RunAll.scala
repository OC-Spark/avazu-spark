package org.ocspark.avazu.ensemble.util

import org.apache.spark.SparkContext

object RunAll {

  def main(args: Array[String]): Unit = {
    
  }
  
  def run(sc : SparkContext){
    org.ocspark.avazu.ensemble.model.app.RunModel.run(sc)
  }

}
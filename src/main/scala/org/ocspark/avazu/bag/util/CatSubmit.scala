package org.ocspark.avazu.bag.util

import org.apache.spark.SparkContext
import org.ocspark.avazu.Common
import org.apache.spark.SparkConf

object CatSubmit {

  def main(args: Array[String]): Unit = {
    val fApp = args(0)	// 'one submission file'
	val fSite = args(1)	// 'the other submission file'
	val fOut = args(2) // 'path to the concantenated submission'
	
	val sparkConf = new SparkConf().setAppName("CatSubmit")
      .setMaster("local[1]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
	run(fApp, fSite, fOut, sc)
	
  }
  
  def run(fApp : String, fSite : String, fOut : String, sc : SparkContext){
    val appRDD = sc.textFile(fApp, 4)
    val siteRDD = sc.textFile(fSite, 4)
    val combinedRDD = appRDD.union(siteRDD)
    Common.writeOut(combinedRDD.collect.mkString("\n"), fOut)
    
  }
  

}
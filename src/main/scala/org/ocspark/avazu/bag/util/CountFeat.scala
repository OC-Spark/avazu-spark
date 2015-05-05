package org.ocspark.avazu.bag.util

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import scala.annotation.meta.field
import org.ocspark.avazu.base.converter.Common

object CountFeat {

  def main(args: Array[String]): Unit = {
    val trPath = args(0)    // tr.r$1.csv 
    val vaPath = args(1)    // va.r$1.csv 
    val minOccur = args(2).toInt    // 2
    val cntPath = args(3)
     
    val sparkConf = new SparkConf().setAppName("Count Features")
      .setMaster("local[4]") // comment out when submitting to spark cluster

    val sc = new SparkContext(sparkConf)
    
    val trRowsHeader = sc.textFile(trPath, 4)
    val trRows = Common.dropHeader(trRowsHeader)
    val vaRowsHeader = sc.textFile(vaPath, 4)
    val vaRows = Common.dropHeader(vaRowsHeader)
    val rows = trRows.union(vaRows)
    
    val headers = trRowsHeader.take(1)(0).split(" ")
    val numColumns = headers.length
    
//    val nr_feats = 0
    
    val cnts = new Array[Map[String, Int]](numColumns)
    for (c <- 0 until numColumns){
      cnts(c) =
      rows.map {
        line => 
          val row = line.split(" ")
          (row(c), 1)
      }
      .reduceByKey(_+_)
      .filter(kv => kv._2 >= minOccur)
      .collect
      .toMap
    }
    
    // store away conts map
    
  }
    
}
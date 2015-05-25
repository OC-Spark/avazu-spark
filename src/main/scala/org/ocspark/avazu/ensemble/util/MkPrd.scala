package org.ocspark.avazu.ensemble.util

import org.apache.spark.SparkContext
import org.ocspark.avazu.Common
import scala.collection.mutable.ArrayBuffer

object MkPrd {

  def main(args: Array[String]): Unit = {
    val csv_path = args(0)
    val out_path = args(1)
    val prd_path = args(2)
    
  }
  
  def run(csv_path : String, out_path : String, prd_path : String, sc : SparkContext){
    val csvLines = sc.textFile(csv_path, 4)
    .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }	// strip header
    .map {
      line =>
        line.split(",", 2)(0)
    }
    val outLines = sc.textFile(out_path, 4)		// no header
    .map {
      prd =>
        prd.toDouble
    }
    val id_prd = Common.genPerEventPrediction(csvLines, outLines)
    
    val idPrdLines = id_prd.map {
    	id_prd =>
    	  id_prd._1 + "," + id_prd._2
    }
    
    Common.writeOut(Array[String]("id", "click"), idPrdLines.collect, prd_path)
    
  }

}
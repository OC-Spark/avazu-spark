package org.ocspark.avazu.ensemble.util

import org.apache.spark.SparkContext
import org.ocspark.avazu.Common

object CalcLoss {

  def main(args: Array[String]): Unit = {
    val prd_path = args(0)
    val ans_path = args(1)
  }
  
  def run(prd_path : String, ans_path : String, sc : SparkContext) : String = {
//    val prdClick = sc.textFile(prd_path, 4)
    val prdClick = sc.textFile(prd_path, 4).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    .map {
      line =>
        val row = line.split(",")
        val id = row(0)
        val click = row(1)
        (id, click)
    }
    val label = sc.textFile(ans_path, 4)
    .map {
      line =>
        val row = line.split(",")
        val id = row(0)
        val click = row(1)
        (id, click)
    }
    val logLossRDD = label.join(prdClick)
    .map {
      line =>
        val id = line._1
        val label_prd = line._2
        val label = label_prd._1
        val prd = label_prd._2
//        println("prd=" + prd)
        var loss = 0.0
        if (label == 1){
	        loss += math.log(prd.toDouble)
        } else {
	        loss += math.log(1 - prd.toDouble)
        }
//        println("loss=" + loss)
	    loss
    }
    val total = logLossRDD.count
    var sumLogLoss = 0.0
    logLossRDD.collect.foreach(l => sumLogLoss += l)
//    println("sumLogLoss = " + sumLogLoss + ", total = " + total)
    val logLoss = -1 * (sumLogLoss / total)
    val lossString = f"$logLoss%1.5f"
    
    println("log loss = " + lossString)
    
    lossString
  }
  
}
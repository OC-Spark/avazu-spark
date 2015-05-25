package org.ocspark.avazu.ensemble.util

import org.apache.spark.SparkContext

object CalcLoss2 {

  def main(args: Array[String]): Unit = {
    val prd_path = args(0)
    val base_path = args(1)
    val ans_path = args(2)
  }
  
  def run(prd_path : String, base_path : String, ans_path : String, sc : SparkContext){
    val prdClick = sc.textFile(prd_path, 4)
    .map {
      line =>
        val row = line.split(",")
        val id = getRowField("id", row)
        val click = getRowField("click", row)
        (id, click)
    }
    val basePrdClick = sc.textFile(base_path, 4)
    .map {
      line =>
        val row = line.split(",")
        val id = getRowField("id", row)
        val click = getRowField("click", row)
        (id, click)
    }
    val label = sc.textFile(ans_path, 4)
    .map {
      line =>
        val row = line.split(",")
        val id = getRowField("id", row)
        val click = getRowField("click", row)
        (id, click)
    }
    val joinBase = prdClick.join(basePrdClick)
    .map {
      line =>
        val id = line._1
        val twoClicks = line._2
        val prd = twoClicks._1
        val base = twoClicks._2
        (id, (prd.toDouble, base.toDouble))
    }
    val logLossRDD = label.join(joinBase)
    .map {
      line =>
        val id = line._1
        val label_prdBase = line._2
        val label = label_prdBase._1
        val prd = label_prdBase._2._1
        val base = label_prdBase._2._2
        var loss = 0.0
        var baseLoss = 0.0
        if (label == 1){
	        loss += math.log(prd)
	        baseLoss += math.log(base)
        } else {
	        loss += math.log(1 - prd)
	        baseLoss += math.log(1 - base)
        }
	    (loss, baseLoss)
    }
    val total = logLossRDD.count
    var sumLogLoss = 0.0
    var sumBaseLogLoss = 0.0
    logLossRDD.collect.foreach {
      l =>
        sumLogLoss += l._1
        sumBaseLogLoss += l._2
    }
    val logLoss = sumLogLoss / total
    val lossString = f"$logLoss%0.5f"
    
    println("log loss = " + lossString)
    
    val baseLogLoss = sumBaseLogLoss / total
    println("base log loss = " + baseLogLoss)
    
  }

  def getRowField(field: String, row: Array[String]): String = {
    row(GenData.simpleFieldMap.getOrElse(field, 0))
  }

}
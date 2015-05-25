package org.ocspark.avazu.ensemble

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import scala.collection.mutable.ArrayBuffer
import org.ocspark.avazu.Common
import org.ocspark.avazu.ensemble.util.GenData
import org.apache.spark.rdd.RDD

abstract class Cvt extends Serializable {
  val NR_BINS = 1000000
  val tf = new HashingTF(NR_BINS)
  var headerMap = Map[String, Int]()

  def convert(src_path: String, dst_path: String, is_train: Boolean, sc: SparkContext) {
  }
  
  def readSrcData(src_path : String, sc : SparkContext) : RDD[Array[String]] = {
    val lines = sc.textFile(src_path, 4)
    val headers = lines.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.take(1) else iter.take(0) }
      .map {
        line =>
          line.split(",")
      }.flatMap(h => h)
      .collect.toList

    val headerHashMap = collection.mutable.HashMap[String, Int]()
    var i = 0
    headers.foreach {
      header =>
        headerHashMap.put(header, i)
        i += 1
    }
    headerMap = headerHashMap.toMap
    
    val srcData = lines.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }.map { line => line.split(",")}
    srcData
  }

  def genHourFeature(row: Array[String], feats: ArrayBuffer[String]) {
    val hourField = row(headerMap("hour"))
    feats.append(tf.indexOf("hour-" + hourField.substring(hourField.length - 2)).toString)
  }

  def genCntFeature(field: String, threshold: Int, row: Array[String], feats: ArrayBuffer[String]) {
    if (row(headerMap(field + "_cnt")).toInt > threshold) {
      feats.append(tf.indexOf(field + "-" + row(headerMap(field + "_cnt"))).toString)
    } else {
      feats.append(tf.indexOf(field + "-less-" + row(headerMap(field + "_cnt"))).toString)
    }
  }
  
  def genUserHourCnt(threshold: Int, row: Array[String], feats: ArrayBuffer[String]) {
    if (row(headerMap("user_hour_cnt")).toInt > threshold) {
      feats.append(tf.indexOf("user_hour_cnt-0").toString)
    } else {
      feats.append(tf.indexOf("user_hour_cnt-" + row(headerMap("user_hour_cnt"))).toString)
    }
  }
  
  def genUserHistoryFeature(threshold: Int, row: Array[String], feats: ArrayBuffer[String]) {
    if (row(headerMap("user_cnt")).toInt > threshold) {
      feats.append(tf.indexOf("user_click_history-" + row(headerMap("user_cnt"))).toString)
    } else {
      feats.append(tf.indexOf("user_click_history-" + row(headerMap("user_cnt")) + "-" + row(headerMap("user_click_history"))).toString)
    } 
  }
  
  def genUserHistory2Feature(threshold: Int, row: Array[String], feats: ArrayBuffer[String]) {
    if (row(headerMap("user_cnt")).toInt > threshold) {
      feats.append(tf.indexOf("user_click_history-" + row(headerMap("user_cnt"))).toString)
    } else if (Common.has_id_info(row(headerMap("device_id")))) {
      feats.append(tf.indexOf("user_click_history-" + row(headerMap("user_cnt")) + "-" + row(headerMap("user_click_history"))).toString)
    } else {
      feats.append(tf.indexOf("user_click_history2-" + row(headerMap("user_cnt")) + "-" + row(headerMap("user_click_history2"))).toString)
    }
  }
  
  def genSplCntFeature(threshold: Int, row: Array[String], feats: ArrayBuffer[String]) {
    if (row(headerMap("spl_cnt")).toInt > threshold) {
      feats.append(tf.indexOf("spl-log-" + math.log(row(headerMap("spl_cnt")).toDouble)).toInt.toString)
    } else {
      feats.append(tf.indexOf("spl-" + row(headerMap("spl_cnt"))).toString)
    }
  }
  
}